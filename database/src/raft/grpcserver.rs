use log::info;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use tonic::{Request, Response, Status};

use crate::{errors::NullDbReadError, file::record::Record};

use super::raft::{AppendEntriesReply, AppendEntriesRequest, VoteReply, VoteRequest};

pub struct RaftGRPCServer {
    pub event_sender: Sender<RaftEvent>,
}

pub enum RaftEvent {
    VoteRequest(VoteRequest, oneshot::Sender<VoteReply>),
    AppendEntriesRequest(AppendEntriesRequest, oneshot::Sender<AppendEntriesReply>),
    NewEntry {
        key: String,
        value: String,
        sender: oneshot::Sender<Result<(), NullDbReadError>>,
    },
    GetEntry(String, oneshot::Sender<Result<Record, NullDbReadError>>),
}

#[tonic::async_trait]
impl super::raft::raft_server::Raft for RaftGRPCServer {
    async fn vote(&self, request: Request<VoteRequest>) -> Result<Response<VoteReply>, Status> {
        info!("Got a request: {:?}", request);
        let (sender, receiver) = oneshot::channel();
        let res = self
            .event_sender
            .send(RaftEvent::VoteRequest(request.into_inner(), sender))
            .await
            .map_err(|_| Status::internal("Failed to send vote request"));
        if let Err(_) = res {
            return Err(Status::internal("Failed to send vote request"));
        };
        return Ok(Response::new(
            receiver
                .await
                .map_err(|_| Status::internal("Failed to receive vote reply"))?,
        ));
    }

    async fn append_entries(
        &self,
        request: Request<AppendEntriesRequest>,
    ) -> Result<Response<AppendEntriesReply>, Status> {
        println!("Got a request: {:?}", request);
        let (sender, receiver) = oneshot::channel();
        let _ = self
            .event_sender
            .send(RaftEvent::AppendEntriesRequest(
                request.into_inner(),
                sender,
            ))
            .await
            .map_err(|_| Status::internal("Failed to send append entries request"));
        return Ok(Response::new(receiver.await.map_err(|_| {
            Status::internal("Failed to receive append entries reply")
        })?));
    }
}
