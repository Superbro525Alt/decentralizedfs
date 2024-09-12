mod chunk;
mod node;
mod network;

/*--------------*/

use std::{fs, io::{self, Write}, thread, time::Duration};
use chunk::{chunk_file, recompile_file, ChunkSize};
use log::{error, info};
use node::{create_node, request_file, verify_integrity, Node};
use uuid::Uuid; 

const NUM_NODES: usize = 2;
const FILES: [&str; 4] = ["file1.dat", "file2.dat", "file3.dat", "file4.dat"];
const FILE_SIZE: usize = 10 * 1024; // 10KB

fn create_files() {
    for (i, &file_name) in FILES.iter().enumerate() {
        if !fs::metadata(file_name).is_ok() {
            let mut file = fs::File::create(file_name).expect("Failed to create file");
            let content = vec![(i as u8 + b'A'); FILE_SIZE]; // Fill each file with repeating pattern (A, B, C, D)
            file.write_all(&content).expect("Failed to write to file");
            info!("Created file: {}", file_name);
        } else {
            info!("File {} already exists", file_name);
        }
    }
}

fn main() {
    env_logger::init();

    create_files();

     // Create 20 nodes with different ports and link them as peers
    let mut nodes = Vec::new();
    for i in 0..NUM_NODES {
        let port = 8080 + i as u16;
        let peer_ports: Vec<String> = (0..NUM_NODES)
            .filter(|&p| p != i)
            .map(|p| format!("127.0.0.1:{}", 8080 + p as u16))
            .collect();

        println!("{:?}", peer_ports);
        let node = create_node("127.0.0.1", port, peer_ports);
        nodes.push(node);
    }

    info!("Created nodes");

    // Distribute 4 different files across the nodes
    let chunk_size = ChunkSize::KB1;
    let mut file_uuids = Vec::new();

    for (i, &file) in FILES.iter().enumerate() {
        info!("Distributing files ({})", i);
        let node_index = i % NUM_NODES; // Choose the node to distribute the file
        let node = nodes[node_index].clone();
        info!("Selected node");

        let file_uuid = node.chunk_and_distribute_file(file, chunk_size).unwrap();

        info!("File UUID: {}", file_uuid);
        file_uuids.push(file_uuid);
        thread::sleep(Duration::from_secs(2)); // Allow time for distribution
    }

    // Recombine the files on all nodes and check integrity
    for (i, &file_uuid) in file_uuids.iter().enumerate() {
        let original_file = FILES[i];
        for node_index in 0..NUM_NODES {
            let output_file = format!("recombined_file_node{}_{}.dat", node_index + 1, i + 1);
            let node = nodes[node_index].clone();

            if let Err(e) = request_file(node, file_uuid, &output_file) {
                error!("Error recompiling file on node {}: {}", node_index + 1, e);
            } else {
                if verify_integrity(original_file, &output_file) {
                    info!("Node {} successfully recombined file {}", node_index + 1, original_file);
                } else {
                    error!("Node {} failed integrity check for file {}", node_index + 1, original_file);
                }
            }
        }
    }
}
