mod chunk;
mod node;
mod network;

/*--------------*/

use std::{io, thread, time::Duration};
use chunk::{chunk_file, recompile_file, ChunkSize};
use log::error;
use node::Node;
use uuid::Uuid; 

fn main() {
    env_logger::init();

    let mut node1 = Node::new("127.0.0.1".to_string(), 8080);
    let mut node2 = Node::new("127.0.0.1".to_string(), 8081);
    // let mut node3 = Node::new("127.0.0.1".to_string(), 8082); 

    node1.add_peer("127.0.0.1:8081".to_string());
    // node1.add_peer("127.0.0.1:8082".to_string());

    node2.add_peer("127.0.0.1:8080".to_string());
    // node2.add_peer("127.0.0.1:8082".to_string());

    // node3.add_peer("127.0.0.1:8080".to_string());
    // node3.add_peer("127.0.0.1:8081".to_string());

    let node1_clone = node1.clone();
    let node2_clone = node2.clone();
    // let node3_clone = node3.clone(); 

    thread::spawn(move || {
        node1_clone.start_listener();
    });

    thread::spawn(move || {
        node2_clone.start_listener();
    });

    // thread::spawn(move || {
    //     node3_clone.start_listener();
    // });

    thread::sleep(Duration::from_millis(500));

    let file_path = "large_file.dat"; 
    let chunk_size = chunk::ChunkSize::KB1; 

    let uid = node1.chunk_and_distribute_file(file_path, chunk_size);
    if let Err(e) = uid { return; }

    thread::sleep(Duration::from_millis(1000));

    let output_file = "recombined_file_node";
    
    if let Err(e) = node1.recompile_file(*uid.as_ref().unwrap(), &format!("{}1.dat", output_file)) {
        error!("Error recompiling file on node1: {}", e);
        return;
    }

    if let Err(e) = node2.recompile_file(*uid.as_ref().unwrap(), &format!("{}2.dat", output_file)) {
        error!("Error recompiling file on node2: {}", e);
        return;
    }

    // if let Err(e) = node3.recompile_file(*uid.as_ref().unwrap(), &format!("{}3.dat", output_file)) {
    //     error!("Error recompiling file on node3: {}", e);
    //     return;
    // }

    // loop {}
}
