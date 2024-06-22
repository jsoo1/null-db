pub mod file_engine;
pub mod record;

pub mod proto {
    tonic::include_proto!("raft");
}
