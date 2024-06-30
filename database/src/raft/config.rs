pub mod raft {
    tonic::include_proto!("raft");
}

#[derive(Default, Clone)]
pub struct RaftConfig {
    pub roster: Vec<String>,
    pub candidate_id: String,
}
