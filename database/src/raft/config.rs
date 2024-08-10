pub mod raft {
    tonic::include_proto!("raft");
}

#[derive(Default, Clone)]
pub struct RaftConfig {
    pub roster: Option<Vec<String>>,
    pub candidate_id: String,
}
