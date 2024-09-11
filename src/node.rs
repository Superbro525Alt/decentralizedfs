use std::error::Error;
use std::fs::{self, File};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::thread;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use serde::{Serialize, Deserialize};
use uuid::Uuid;
use std::io::{self, Read, Write};
use std::collections::HashMap;
use bincode;
use log::{info, warn, error};

use crate::chunk;
use crate::network::Message;

#[derive(Clone)]
pub struct Node {
    pub uuid: Uuid,
    ip: String,
    port: u16,
    pub files: Arc<Mutex<HashMap<Uuid, SplitFile>>>, 
    peers: Vec<String>, 
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SplitFile {
    file_name: String,
    total_chunks: usize,
    stored_chunks: HashMap<usize, Vec<u8>>, 
    uuid: Uuid,
    parity_chunks: usize, // Number of parity chunks generated
    is_complete: bool, // Whether the file is complete (all chunks received)
}

impl Node {
    pub fn new(ip: String, port: u16) -> Self {
        Node {
            uuid: Uuid::new_v4(),
            ip,
            port,
            files: Arc::new(Mutex::new(HashMap::new())),
            peers: Vec::new(),
        }
    }

    pub fn start_listener(&self) {
        let listener_address = format!("{}:{}", self.ip, self.port);
        let listener = TcpListener::bind(listener_address).expect("Could not bind listener");

        info!("Node listening on port {}", self.port);

        let storage_dir = self.create_storage_directory().unwrap(); 

        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let files = self.files.clone();
                    let storage_dir_clone = storage_dir.clone(); 
                    thread::spawn(move || {
                        handle_connection(stream, files, &storage_dir_clone);
                    });
                }
                Err(e) => {
                    error!("Failed to accept connection: {}", e);
                }
            }
        }
    }

    fn create_storage_directory(&self) -> io::Result<PathBuf> {
        let mut storage_path = PathBuf::from("./decentralizedfs"); 
        storage_path.push(self.uuid.to_string()); 

        if !storage_path.exists() {
            fs::create_dir_all(&storage_path)?;
            info!("Created storage directory at {:?}", storage_path);
        }

        Ok(storage_path)
    }

    pub fn connect_to_peer(&self, peer_address: &str, message: Message) -> io::Result<()> {
        match TcpStream::connect(peer_address) {
            Ok(mut stream) => {
                message.send(&mut stream)?;
                info!("Connected to peer at {}", peer_address);
                Ok(())
            }
            Err(e) => {
                error!("Failed to connect to peer {}: {}", peer_address, e);
                Err(e)
            }
        }
    }

    pub fn request_chunk(&self, peer_address: &str, file_uuid: Uuid, chunk_index: usize) -> io::Result<()> {
        let message = Message::ChunkRequest { file_uuid, chunk_index };
        self.connect_to_peer(peer_address, message)?;
        Ok(())
    }

    pub fn add_peer(&mut self, peer_address: String) {
        self.peers.push(peer_address);
    }

    // Sync with peers: Request metadata and missing chunks
    pub fn sync_with_peer(&self, peer_address: &str, file_uuid: Uuid) -> io::Result<()> {
        let message = Message::Sync { file_uuid };
        self.connect_to_peer(peer_address, message)?;
        Ok(())
    }

    // Handle the FileComplete message: Broadcast to peers that file is complete
    pub fn broadcast_file_complete(&self, file_uuid: Uuid) -> io::Result<()> {
        let message = Message::FileComplete { file_uuid };
        for peer in &self.peers {
            self.connect_to_peer(peer, message.clone())?;
            info!("Notified peer {} that file {} is complete", peer, file_uuid);
        }
        Ok(())
    }

    pub fn chunk_and_distribute_file(&self, file_path: &str, chunk_size: chunk::ChunkSize) -> io::Result<Uuid> {
        let storage_dir = self.create_storage_directory()?; 

        let chunk_collection = chunk::chunk_file(file_path, chunk_size, &storage_dir)?;
        let num_parity_chunks = chunk_collection.parity_chunks;
        let total_chunks = chunk_collection.num_chunks + num_parity_chunks;

        info!("Total chunks created: {}, Parity chunks: {}", chunk_collection.num_chunks, num_parity_chunks);

        let peers_count = self.peers.len();
        if peers_count == 0 {
            warn!("No peers to distribute chunks to.");
        }

        let mut chunk_index = 0;
        for peer in &self.peers {
            for i in (chunk_index..chunk_collection.num_chunks).step_by(peers_count) {
                let chunk_filename = format!("{}_chunk_{}.dat", chunk_collection.uuid, i);
                let chunk_path = storage_dir.join(&chunk_filename);

                if let Ok(mut chunk_file) = File::open(&chunk_path) {
                    let mut buffer = Vec::new();
                    chunk_file.read_to_end(&mut buffer)?;

                    match self.connect_to_peer(peer, Message::ChunkResponse {
                        file_uuid: chunk_collection.uuid,
                        chunk_index: i,
                        chunk_data: buffer.clone(),
                        total_chunks: total_chunks,
                        parity_chunks: num_parity_chunks,
                        is_parity_chunk: false,
                    }) {
                        Ok(_) => info!("Sent chunk {} to peer {}", i, peer),
                        Err(e) => error!("Failed to send chunk {} to peer {}: {}", i, peer, e),
                    }
                }
            }
            chunk_index += 1;
        }

        let parity_chunk_filename = format!("{}_chunk_parity.dat", chunk_collection.uuid);
        let parity_chunk_path = storage_dir.join(&parity_chunk_filename);

        if let Ok(mut parity_chunk_file) = File::open(&parity_chunk_path) {
            info!("Sending Parity Chunk");
            let mut buffer = Vec::new();
            parity_chunk_file.read_to_end(&mut buffer)?;

            for peer in &self.peers {
                match self.connect_to_peer(peer, Message::ChunkResponse {
                    file_uuid: chunk_collection.uuid,
                    chunk_index: chunk_collection.num_chunks,
                    chunk_data: buffer.clone(),
                    total_chunks,
                    parity_chunks: num_parity_chunks,
                    is_parity_chunk: true,
                }) {
                    Ok(_) => info!("Sent parity chunk to peer {}", peer),
                    Err(e) => error!("Failed to send parity chunk to peer {}: {}", peer, e),
                }
            }

            self.store_chunk(
                chunk_collection.uuid,
                chunk_collection.num_chunks,
                buffer,
                total_chunks,
                num_parity_chunks,
                true,
            );
            info!("Stored parity chunk locally");
        }

        for index in chunk_index..chunk_collection.num_chunks {
            let chunk_filename = format!("{}_chunk_{}.dat", chunk_collection.uuid, index);
            let chunk_path = storage_dir.join(&chunk_filename);

            if let Ok(mut chunk_file) = File::open(&chunk_path) {
                let mut buffer = Vec::new();
                chunk_file.read_to_end(&mut buffer)?;
                self.store_chunk(chunk_collection.uuid, index, buffer, total_chunks, num_parity_chunks, false);
                info!("Stored chunk {} locally", index);
            }
        }

        self.broadcast_file_complete(chunk_collection.uuid)?;  // Broadcast file completion

        Ok(chunk_collection.uuid)
    }

    fn store_chunk(&self, file_uuid: Uuid, chunk_index: usize, chunk_data: Vec<u8>, total_chunks: usize, parity_chunks: usize, is_parity_chunk: bool) {
        let mut files_lock = self.files.lock().unwrap();
        if let Some(file) = files_lock.get_mut(&file_uuid) {
            file.stored_chunks.insert(chunk_index, chunk_data.clone());
            file.total_chunks = total_chunks;
            file.parity_chunks = parity_chunks;
        } else {
            let mut stored_chunks = HashMap::new();
            stored_chunks.insert(chunk_index, chunk_data.clone());

            let new_file = SplitFile {
                file_name: "unknown".to_string(),
                total_chunks,
                stored_chunks,
                uuid: file_uuid,
                parity_chunks,
                is_complete: false, // Mark as incomplete until all chunks are received
            };

            files_lock.insert(file_uuid, new_file);
        }

        if let Err(e) = self.store_chunk_in_file(file_uuid, chunk_index, chunk_data) {
            error!("Failed to store chunk {} for file {}: {}", chunk_index, file_uuid, e);
        }
    }

    fn store_chunk_in_file(&self, file_uuid: Uuid, chunk_index: usize, chunk_data: Vec<u8>) -> io::Result<()> {
        let storage_path = self.create_storage_directory()?; 

        let chunk_filename = format!("{}_chunk_{}.dat", file_uuid, chunk_index);
        let chunk_path = storage_path.join(&chunk_filename); 

        let mut chunk_file = File::create(&chunk_path)?;
        chunk_file.write_all(&chunk_data)?;
        info!("Stored chunk {} for file {} at {:?}", chunk_index, file_uuid, chunk_path);

        Ok(())
    }

    fn request_missing_chunks(&self, file_uuid: Uuid, total_chunks: usize, files: HashMap<Uuid, SplitFile>) -> io::Result<()> {
        info!("Requesting missing chunks for file {} (total: {})", file_uuid, total_chunks);
        for chunk_index in 0..total_chunks {
            if let Some(file) = files.get(&file_uuid) {
                if !file.stored_chunks.contains_key(&chunk_index) {
                    for peer in &self.peers {
                        info!("Requesting chunk {} for file {} from peer {}", chunk_index, file_uuid, peer);
                        self.request_chunk(peer, file_uuid, chunk_index)?;
                    }
                }
            }
        }
        Ok(())
    }

    pub fn recompile_file(&self, file_uuid: Uuid, output_file: &str) -> io::Result<()> {
        let files = self.files.lock().unwrap();
        if let Some(file) = files.get(&file_uuid) {
            info!("Recompiling file with UUID: {} (node uuid: {})", file_uuid, self.uuid);

            // Request missing chunks (both data and parity)
            self.request_missing_chunks(file_uuid, file.total_chunks, files.clone())?;
            thread::sleep(Duration::from_secs(3)); 

            let expected_chunks = file.total_chunks;
            let stored_chunks_count = file.stored_chunks.len();

            info!(
                "Stored chunks for file: {}. Expected chunks: {}. Parity chunks: {}",
                stored_chunks_count, expected_chunks, file.parity_chunks
            );

            if stored_chunks_count + file.parity_chunks == expected_chunks {
                info!("All chunks are available, starting recompilation");
                let storage_path = self.create_storage_directory()?;
                chunk::recompile_file(file_uuid, output_file, file.total_chunks, &storage_path, file.parity_chunks)
            } else {
                warn!("Not all chunks are available for file {}", file_uuid);
                Err(io::Error::new(io::ErrorKind::NotFound, "Not all chunks are available for recombination"))
            }
        } else {
            error!("File {} not found", file_uuid);
            Err(io::Error::new(io::ErrorKind::NotFound, "File not found"))
        }
    }
}

fn handle_chunk_response(
    files: Arc<Mutex<HashMap<Uuid, SplitFile>>>,
    file_uuid: Uuid,
    chunk_index: usize,
    chunk_data: Vec<u8>,
    path: &PathBuf,
    total_chunks: usize,
    parity_chunks: usize,
    is_parity_chunk: bool,
) {
    let mut files_lock = files.lock().unwrap();
    if !is_parity_chunk {
        if let Some(file) = files_lock.get_mut(&file_uuid) {
            file.stored_chunks.insert(chunk_index, chunk_data.clone());
            file.total_chunks = total_chunks;
            file.parity_chunks = parity_chunks;
            info!("Stored chunk {} for file {}", chunk_index, file_uuid);
        } else {
            let mut stored_chunks = HashMap::new();
            stored_chunks.insert(chunk_index, chunk_data.clone());

            let new_file = SplitFile {
                file_name: "unknown".to_string(),
                total_chunks: 0,
                stored_chunks,
                uuid: file_uuid,
                parity_chunks: 0,
                is_complete: false
            };

            files_lock.insert(file_uuid, new_file);
            info!("Created new file entry and stored chunk {} for file {}", chunk_index, file_uuid);
        }
    }

    let node_storage_path = path;
    if !node_storage_path.exists() {
        if let Err(e) = fs::create_dir_all(&node_storage_path) {
            error!("Failed to create storage directory for node {}: {}", file_uuid, e);
            return;
        }
    }

    let chunk_filename = if is_parity_chunk {
        format!("{}_chunk_parity.dat", file_uuid)
    } else {
        format!("{}_chunk_{}.dat", file_uuid, chunk_index)
    };

    let chunk_path = node_storage_path.join(&chunk_filename);

    if let Err(e) = File::create(&chunk_path).and_then(|mut file| file.write_all(&chunk_data)) {
        error!("Failed to write chunk {} for file {} to disk: {}", chunk_index, file_uuid, e);
    } else {
        info!("Successfully saved chunk {} for file {} to {:?}", chunk_index, file_uuid, chunk_path);
    }
}

fn handle_connection(mut stream: TcpStream, files: Arc<Mutex<HashMap<Uuid, SplitFile>>>, path: &PathBuf) {
    match Message::receive(&mut stream) {
        Ok(Message::ChunkRequest { file_uuid, chunk_index }) => {
            let files_lock = files.lock().unwrap();
            if let Some(file) = files_lock.get(&file_uuid) {
                if let Some(chunk) = file.stored_chunks.get(&chunk_index) {
                    let response = Message::ChunkResponse {
                        file_uuid,
                        chunk_index,
                        chunk_data: chunk.clone(),
                        total_chunks: file.total_chunks,
                        parity_chunks: file.parity_chunks,
                        is_parity_chunk: false,
                    };
                    response.send(&mut stream).expect("Failed to send chunk");
                    info!("Sent chunk {} for file {} to peer", chunk_index, file_uuid);
                } else {
                    warn!("Requested chunk {} for file {} is missing", chunk_index, file_uuid);
                }
            }
        }
        Ok(Message::ChunkResponse { file_uuid, chunk_index, chunk_data, total_chunks, parity_chunks, is_parity_chunk }) => {
            info!("Received chunk {} for file {}", chunk_index, file_uuid);
            handle_chunk_response(files.clone(), file_uuid, chunk_index, chunk_data, path, total_chunks, parity_chunks, is_parity_chunk);
        }
        Ok(Message::FileComplete { file_uuid }) => {
            info!("File {} is complete", file_uuid);
            let mut files_lock = files.lock().unwrap();
            if let Some(file) = files_lock.get_mut(&file_uuid) {
                file.is_complete = true;  // Mark the file as complete
                info!("File {} is marked as complete", file_uuid);
            }
        }
        Ok(Message::Sync { file_uuid }) => {
            info!("Sync request for file {}", file_uuid);
            let files_lock = files.lock().unwrap();
            if let Some(file) = files_lock.get(&file_uuid) {
                // Sync logic: request missing chunks or metadata
                info!("Syncing file {} with peer", file_uuid);
                // Implement sync logic here...
            }
        }
        Err(e) => error!("Error handling connection: {}", e),
    }
}

pub fn create_node(ip: &str, port: u16, peer_addresses: Vec<String>) -> Node {
    // Initialize the node with the given IP and port, wrapped in a Mutex for mutable access
    let mut node = Node::new(ip.to_string(), port);

    // Clone the Arc so we can move it into the thread
    let node_clone = node.clone();

    // Start the node listener in a separate thread
    thread::spawn(move || {
        // let node_locked = node_clone.lock().unwrap();
        node_clone.start_listener();
    });

    // Add peers to the node
    {
        for peer_address in peer_addresses {
            node.add_peer(peer_address);
        }
    }

    // Give some time for the listener to start and peers to connect
    thread::sleep(Duration::from_secs_f64(0.2));

    node
}

pub fn request_file_from_network(node: Arc<Mutex<Node>>, file_uuid: Uuid, output_file: &str) -> Result<(), Box<dyn Error>> {
    // Sync with all peers to request any missing chunks or metadata
    {
        let node_locked = node.lock().unwrap();
        for peer in &node_locked.peers {
            node_locked.sync_with_peer(peer, file_uuid)?;
        }
    }

    // Give some time for peers to respond with missing chunks
    thread::sleep(Duration::from_secs(5));

    // Attempt to recompile the file once all chunks (data + parity) are available
    {
        let mut node_locked = node.lock().unwrap();
        node_locked.recompile_file(file_uuid, output_file)?;
    }

    Ok(())
}

pub fn request_file(node: Node, file_uuid: Uuid, output_file: &str) -> Result<(), Box<dyn Error>> {
    node.recompile_file(file_uuid, output_file)?;
    Ok(())
}

pub fn verify_integrity(original_file: &str, recombined_file: &str) -> bool {
    let original_content = fs::read(original_file).unwrap_or_default();
    let recombined_content = fs::read(recombined_file).unwrap_or_default();
    original_content == recombined_content
}

