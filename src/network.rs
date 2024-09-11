use serde::{Serialize, Deserialize};
use uuid::Uuid;
use std::net::TcpStream;
use std::io::{self, Write, Read};
use bincode;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Message {
    ChunkRequest { file_uuid: Uuid, chunk_index: usize },
    ChunkResponse { file_uuid: Uuid, chunk_index: usize, chunk_data: Vec<u8>, total_chunks: usize, parity_chunks: usize, is_parity_chunk: bool },
    FileComplete { file_uuid: Uuid },
    Sync { file_uuid: Uuid },
}

impl Message {
    pub fn send(&self, stream: &mut TcpStream) -> io::Result<()> {
        let encoded_msg = bincode::serialize(self).unwrap();
        stream.write_all(&encoded_msg)?;
        Ok(())
    }

    pub fn receive(stream: &mut TcpStream) -> io::Result<Self> {
        let mut buffer = Vec::new();
        stream.read_to_end(&mut buffer)?;
        let message: Message = bincode::deserialize(&buffer).unwrap();
        Ok(message)
    }
}

