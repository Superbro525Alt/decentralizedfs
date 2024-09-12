mod chunk;
mod node;
mod network;

/*--------------*/

use std::{fs, io::Write};
use chunk::ChunkSize;
use log::{error, info};
use node::{create_node, request_file, verify_integrity};

const NUM_NODES: usize = 2;
const FILE_COUNT: usize = 30;
const FILE_SIZE: usize = 100 * 1024; // 10KB

fn create_files(file_count: usize) {
    for i in 0..file_count {
        let file_name = format!("file_{}.dat", i + 1); // Generate dynamic file names

        if fs::metadata(&file_name).is_err() {
            let mut file = fs::File::create(&file_name).expect("Failed to create file");
            let content = vec![i as u8 + b'A'; FILE_SIZE]; 
            file.write_all(&content).expect("Failed to write to file");
            info!("Created file: {}", file_name);
        } else {
            info!("File {} already exists", file_name);
        }
    }
}

fn main() {
    env_logger::init();

    create_files(FILE_COUNT);

    let mut nodes = Vec::new();
    for i in 0..NUM_NODES {
        let port = 8080 + i as u16;
        let peer_ports: Vec<String> = (0..NUM_NODES)
            .filter(|&p| p != i)
            .map(|p| format!("127.0.0.1:{}", 8080 + p as u16))
            .collect();

        let node = create_node("127.0.0.1", port, peer_ports);
        nodes.push(node);
    }

    let chunk_size = ChunkSize::KB16;
    let mut file_uuids = Vec::new();

    for i in 0..FILE_COUNT {
        let file_name = format!("file_{}.dat", i + 1); // Dynamically generated file names
        let node_index = i % NUM_NODES;
        let node = nodes[node_index].clone();

        let file_uuid = node.chunk_and_distribute_file(&file_name, chunk_size).unwrap();

        file_uuids.push(file_uuid);
    }

    for (i, &file_uuid) in file_uuids.iter().enumerate() {
        let original_file = format!("file_{}.dat", i + 1);
        for node_index in 0..NUM_NODES {
            let output_file = format!("recombined_file_node{}_{}.dat", node_index + 1, i + 1);
            let node = nodes[node_index].clone();

            if let Err(e) = request_file(node, file_uuid, &output_file) {
                error!("Error recompiling file on node {}: {}", node_index + 1, e);
            } else {
                if verify_integrity(&original_file, &output_file) {
                    info!("Node {} successfully recombined file {}", node_index + 1, original_file);
                } else {
                    error!("Node {} failed integrity check for file {}", node_index + 1, original_file);
                }
            }
        }
    }
}
