use log::{error, info, warn};
use serde::{Serialize, Deserialize}; // For serialization and deserialization
use sha2::{Sha256, Digest}; // For hashing
use uuid::Uuid;
use bincode; // For binary serialization
use std::fs::{self, File};
use std::io::{self, Read, Write, BufReader, BufWriter};
use std::path::{Path, PathBuf};
use std::fmt;

#[allow(dead_code)]
#[derive(Clone, Copy)]
pub enum ChunkSize {
    B1 = 1,                              // 2^0 = 1 Byte
    B2 = 1 << 1,                         // 2^1 = 2 Bytes
    B4 = 1 << 2,                         // 2^2 = 4 Bytes
    B8 = 1 << 3,                         // 2^3 = 8 Bytes
    B16 = 1 << 4,                        // 2^4 = 16 Bytes
    B32 = 1 << 5,                        // 2^5 = 32 Bytes
    B64 = 1 << 6,                        // 2^6 = 64 Bytes
    B128 = 1 << 7,                       // 2^7 = 128 Bytes
    B256 = 1 << 8,                       // 2^8 = 256 Bytes
    B512 = 1 << 9,                       // 2^9 = 512 Bytes
    KB1 = 1 << 10,                       // 1 KB
    KB2 = 1 << 11,                       // 2 KB
    KB4 = 1 << 12,                       // 4 KB
    KB8 = 1 << 13,                       // 8 KB
    KB16 = 1 << 14,                      // 16 KB
    KB32 = 1 << 15,                      // 32 KB
    KB64 = 1 << 16,                      // 64 KB
    KB128 = 1 << 17,                     // 128 KB
    KB256 = 1 << 18,                     // 256 KB
    KB512 = 1 << 19,                     // 512 KB
    MB1 = 1 << 20,                       // 1 MB
    MB2 = 1 << 21,                       // 2 MB
    MB5 = (1 << 20) * 5,                 // 5 MB
    MB10 = (1 << 20) * 10,               // 10 MB
    MB50 = (1 << 20) * 50,               // 50 MB
    MB100 = (1 << 20) * 100,             // 100 MB
    MB500 = (1 << 20) * 500,             // 500 MB
    GB1 = 1 << 30,                       // 1 GB
    GB2 = 1 << 31,                       // 2 GB
    GB5 = (1 << 30) * 5,                 // 5 GB
    GB10 = (1 << 30) * 10,               // 10 GB
    GB50 = (1 << 30) * 50,               // 50 GB
    GB100 = (1 << 30) * 100,             // 100 GB
    TB1 = 1 << 40,                       // 1 TB
}

impl ChunkSize {
    fn size(&self) -> usize {
        *self as usize
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ChunkMetadata {
    pub uuid: Uuid,
    pub chunk_index: usize,
    pub total_chunks: usize,
    pub chunk_size: usize,
    pub hash: Vec<u8>, 
}

pub struct ChunkCollection {
    pub uuid: Uuid,
    pub num_chunks: usize,
    pub parity_chunks: usize
}

pub struct Chunk {
    id: String,
    size: ChunkSize,
    original_path: PathBuf,
    chunk_path: PathBuf,
    uuid: Uuid,
    metadata: ChunkMetadata,
}

impl fmt::Display for Chunk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Chunk {{ id: {}, size: {} bytes, original: {:?}, chunk_path: {:?}, uuid: {}, index: {}, total_chunks: {}, hash: {:?} }}",
            self.id,
            self.size.size(),
            self.original_path,
            self.chunk_path,
            self.uuid,
            self.metadata.chunk_index,
            self.metadata.total_chunks,
            &self.metadata.hash[..8] 
        )
    }
}

impl Chunk {
    fn write_chunk(&self, data: &[u8]) -> io::Result<()> {
        let chunk_file = File::create(&self.chunk_path)?;
        let mut writer = BufWriter::new(chunk_file);

        let metadata_bytes = bincode::serialize(&self.metadata).unwrap();
        writer.write_all(&metadata_bytes)?;

        writer.write_all(data)?;
        writer.flush()?;
        Ok(())
    }

    // Compute SHA-256 hash of the chunk data
    pub fn compute_hash(data: &[u8]) -> Vec<u8> {
        let mut hasher = Sha256::new();
        hasher.update(data);
        hasher.finalize().to_vec()
    }

    fn xor_bytes(a: &[u8], b: &[u8]) -> Vec<u8> {
        a.iter().zip(b.iter()).map(|(x, y)| x ^ y).collect()
    }
}

fn chunk_exists(chunk_filename: &str) -> bool {
    Path::new(chunk_filename).exists()
}

pub fn chunk_file(file_path: &str, chunk_size: ChunkSize, storage_dir: &Path) -> io::Result<ChunkCollection> {
    let file_uuid = Uuid::new_v4();
    
    if !storage_dir.exists() {
        fs::create_dir_all(storage_dir)?;
    }

    let input_file = File::open(file_path)?;
    let mut reader = BufReader::new(input_file);

    let mut buffer = vec![0u8; chunk_size.size()];
    let mut chunk_index = 0;
    let original_path = PathBuf::from(file_path);
    let mut total_chunks = 0;
    let mut parity_chunk = vec![0u8; chunk_size.size()]; 

    loop {
        let bytes_read = reader.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }
        total_chunks += 1;
    }

    let input_file = File::open(file_path)?;
    let mut reader = BufReader::new(input_file);

    while let Ok(bytes_read) = reader.read(&mut buffer) {
        if bytes_read == 0 {
            break;
        }

        let chunk_hash = Chunk::compute_hash(&buffer[..bytes_read]);

        let chunk_metadata = ChunkMetadata {
            uuid: file_uuid,
            chunk_index,
            total_chunks,
            chunk_size: chunk_size.size(),
            hash: chunk_hash.clone(),
        };

        let chunk_filename = format!("{}_chunk_{}.dat", file_uuid, chunk_index);
        let chunk_path = storage_dir.join(&chunk_filename);
        let chunk = Chunk {
            id: chunk_filename.clone(),
            size: chunk_size,
            original_path: original_path.clone(),
            chunk_path: chunk_path.clone(),
            uuid: file_uuid,
            metadata: chunk_metadata,
        };

        parity_chunk = Chunk::xor_bytes(&parity_chunk, &buffer[..bytes_read]);

        chunk.write_chunk(&buffer[..bytes_read])?;

        chunk_index += 1;
    }

    let parity_chunk_metadata = ChunkMetadata {
        uuid: file_uuid,
        chunk_index,
        total_chunks: total_chunks + 1, 
        chunk_size: chunk_size.size(),
        hash: Chunk::compute_hash(&parity_chunk),
    };

    let parity_chunk_filename = format!("{}_chunk_parity.dat", file_uuid);
    let parity_chunk_path = storage_dir.join(&parity_chunk_filename);
    let parity_chunk_struct = Chunk {
        id: parity_chunk_filename.clone(),
        size: chunk_size,
        original_path: original_path.clone(),
        chunk_path: parity_chunk_path.clone(),
        uuid: file_uuid,
        metadata: parity_chunk_metadata,
    };

    parity_chunk_struct.write_chunk(&parity_chunk)?;

    Ok(ChunkCollection {
        uuid: file_uuid,
        num_chunks: total_chunks,
        parity_chunks: 1
    })
}

pub fn recompile_file(uuid: Uuid, output_file: &str, num_chunks: usize, storage_dir: &Path, parity_chunks: usize) -> io::Result<()> {
    let output_path = PathBuf::from(output_file);
    let mut output_file = File::create(&output_path)?;
    let mut writer = BufWriter::new(output_file);

    let mut reconstructed_chunk = vec![0u8; ChunkSize::MB10.size()]; 
    let mut missing_chunk_index: Option<usize> = None;

    for chunk_index in 0..num_chunks - parity_chunks {
        let chunk_filename = format!("{}_chunk_{}.dat", uuid, chunk_index);
        let chunk_path = storage_dir.join(&chunk_filename);

        if !chunk_exists(chunk_path.to_str().unwrap()) {
            missing_chunk_index = Some(chunk_index);
            continue;
        }

        let mut chunk_file = File::open(&chunk_path)?;
        let mut reader = BufReader::new(chunk_file);

        let metadata_size = 88;
        let mut metadata_buf = vec![0u8; metadata_size];

        if reader.read_exact(metadata_buf.as_mut_slice()).is_err() {
            continue;
        }

        let chunk_metadata: ChunkMetadata = match bincode::deserialize(&metadata_buf) {
            Ok(metadata) => metadata,
            Err(_) => continue,
        };

        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer)?;

        if missing_chunk_index.is_some() {
            warn!("Missing chunk (attempting to recontruct)");
            reconstructed_chunk = Chunk::xor_bytes(&reconstructed_chunk, &buffer);
        }

        writer.write_all(&buffer)?;
    }

    if let Some(missing) = missing_chunk_index {
        let parity_chunk_filename = format!("{}_chunk_parity.dat", uuid);
        let parity_chunk_path = storage_dir.join(&parity_chunk_filename);

        let mut parity_chunk_file = File::open(&parity_chunk_path)?;
        let mut reader = BufReader::new(parity_chunk_file);

        let mut parity_buffer = Vec::new();
        reader.read_to_end(&mut parity_buffer)?;

        let rebuilt_chunk = Chunk::xor_bytes(&reconstructed_chunk, &parity_buffer);

        writer.write_all(&rebuilt_chunk)?;
    }

    writer.flush()?;
    info!("Recompiled File");
    Ok(())
}
