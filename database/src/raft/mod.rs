mod candidate;
pub mod config;
mod follower;
pub mod grpcserver;
mod leader;
use actix_web::web::Data;
use log::info;
use raft::raft_server::RaftServer;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::mpsc::{Receiver, Sender};
use tonic::{transport::Server, Status};
pub mod raft {
    tonic::include_proto!("raft");
}

use self::{
    candidate::CandidateState, follower::FollowerState, grpcserver::RaftEvent, leader::LeaderState,
};
use crate::{nulldb::NullDB, raft::grpcserver::RaftGRPCServer};
use config::RaftConfig;
const TIME_OUT: Duration = Duration::from_secs(1);

type RaftClients =
    Arc<Mutex<HashMap<String, raft::raft_client::RaftClient<tonic::transport::channel::Channel>>>>;

/// RaftNode is the main struct that represents a Raft node.
/// It contains the state of the node, the RaftClients to connect to other nodes,
/// the log, the configuration and a receiver to receive messages.
pub struct RaftNode {
    state: State,
    raft_clients: RaftClients,
    log: Data<NullDB>,
    config: RaftConfig,
    receiver: Receiver<RaftEvent>,
}

impl RaftNode {
    /// Create a new RaftNode with a configuration, a receiver and a log.
    pub fn new(config: RaftConfig, receiver: Receiver<RaftEvent>, log: Data<NullDB>) -> Self {
        Self {
            state: State::Follower(FollowerState::new(Instant::now(), 0)),
            raft_clients: Arc::new(Mutex::new(HashMap::new())),
            log,
            config,
            receiver,
        }
    }

    /// Run the RaftNode.
    /// This function will start the gRPC server, connect to all other nodes and
    /// start the main loop of the RaftNode.
    pub async fn run(&mut self, sender: Sender<RaftEvent>) -> Result<(), Status> {
        // Start the gRPC server
        let port = self.config.candidate_id.clone();
        tokio::spawn(async move {
            let res = start_raft_server(port, sender).await;
            if let Err(e) = res {
                println!("Error: {:?}", e);
            }
        });

        // Connect to all other nodes
        let raft_clients = self.raft_clients.clone();
        let config = self.config.clone();
        tokio::spawn(async move {
            for node in config.roster.clone() {
                let nameport = node.split(":").collect::<Vec<&str>>();
                let ip = format!(
                    "http://{}:{}",
                    nameport[0].to_string(),
                    nameport[1].to_string()
                );
                info!("Connecting to {}", ip);

                // try to connect to the node
                // if it fails, the node is not up yet
                // so we will try again in the next iteration
                let raft_clients_clone = raft_clients.clone();
                tokio::spawn(async move {
                    loop {
                        let raft_client = raft::raft_client::RaftClient::connect(ip.clone()).await;
                        if let Ok(raft_client) = raft_client {
                            {
                                let mut raft_clients = raft_clients_clone.lock().unwrap();
                                raft_clients.insert(node.to_string(), raft_client);
                            }
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                });
            }
        });

        // Main loop
        loop {
            tokio::time::sleep(Duration::from_millis(25)).await;
            let state = self.run_tick().await;
            self.next_state(state);
        }
    }

    /// Run a tick of the RaftNode.
    /// This function will check if there are any messages in the receiver and
    /// call the tick function of the current state.
    /// It will return a new state if a transition is needed.
    /// If no transition is needed, it will return None.
    async fn run_tick(&mut self) -> Option<State> {
        // Check if there are any messages in the receiver
        // These messages are from the other nodes.
        // If there are messages, call the on_message function of the current state.
        match self.receiver.try_recv() {
            Ok(event) => {
                info!("Got a message");
                self.state
                    .on_message(
                        event,
                        &self.config,
                        self.raft_clients.clone(),
                        self.log.clone(),
                    )
                    .await
            }
            Err(_) => None,
        };

        // Call the tick function of the current state
        // Tick will check the nodes understanding of the current state of the cluster
        let state = self
            .state
            .tick(&self.config, self.log.clone(), self.raft_clients.clone())
            .await;
        if state.is_some() {
            return state;
        }

        // If no state transition is needed, return None
        None
    }

    /// Transition to a new state if needed.
    fn next_state(&mut self, state: Option<State>) {
        if let Some(state) = state {
            self.state = state;
        }
    }
}

/// The state of the Raft node.
/// The state is responsible for handling messages and timeouts.
pub enum State {
    Follower(FollowerState),
    Candidate(CandidateState),
    Leader(LeaderState),
}

impl State {
    /// Handle a RaftEvent message.
    pub async fn on_message(
        &mut self,
        message: RaftEvent,
        config: &RaftConfig,
        clients: RaftClients,
        log: Data<NullDB>,
    ) -> Option<State> {
        match self {
            State::Follower(follower) => follower.on_message(message, log),
            State::Candidate(candidate) => candidate.on_message(message, log),
            State::Leader(leader) => leader.on_message(message, config, clients, log).await,
        }
    }

    /// Tick checks the current status of the node
    /// optionally returns a state if a transition is needed.
    pub async fn tick(
        &mut self,
        config: &RaftConfig,
        log: Data<NullDB>,
        clients: RaftClients,
    ) -> Option<State> {
        match self {
            State::Follower(follower) => follower.tick(),
            State::Candidate(candidate) => candidate.tick(config, log, clients).await,
            State::Leader(leader) => leader.tick(config, clients).await,
        }
    }
}

/// Start raft gRPC server
pub async fn start_raft_server(
    port: String,
    sender: Sender<RaftEvent>,
) -> Result<(), Box<dyn std::error::Error>> {
    let raft_server = RaftGRPCServer {
        event_sender: sender,
    };
    let addr = format!("0.0.0.0:{}", port).parse().unwrap();
    let server = RaftServer::new(raft_server);
    Server::builder().add_service(server).serve(addr).await?;
    println!("Raft server listening on: {}", addr);
    Ok(())
}
