use std::time::Instant;
use log::info;
use actix_web::web::Data;
use crate::{raft::{candidate::CandidateState, raft}, nulldb::NullDB, errors::NullDbReadError};
use super::{grpcserver::RaftEvent, State, TIME_OUT};

pub struct FollowerState {
    pub last_heartbeat: Instant,
    pub term: u32,
}

impl FollowerState {
    pub fn new(last_heartbeat: Instant, term: u32) -> FollowerState {
        FollowerState {
            last_heartbeat,
            term,
        }
    }

    pub fn tick(&mut self) -> Option<State> {
        if self.last_heartbeat.elapsed() > TIME_OUT {
            println!(
                "Becoming Candidate. Failed to get heartbeat from leader. +++++++!!!!!!!!!+++++++"
            );
            return Some(State::Candidate(CandidateState::new(self.term)));
        }
        None
    }

    pub fn on_message(&mut self, message: RaftEvent, log: Data<NullDB>) -> Option<State> {
        match message {
            RaftEvent::VoteRequest(request, sender) => {
                info!("Got a vote request: {:?}", request);
                if request.term >= self.term {
                    println!("voting yes");
                    //TODO: Do we up the term now? or do we wait until we get a heartbeat?
                    // term = request.term;
                    let reply = raft::VoteReply {
                        term: self.term,
                        vote_granted: true,
                    };
                    sender.send(reply).unwrap();
                    return Some(State::Follower(FollowerState::new(
                        Instant::now(),
                        self.term,
                    )));
                }
                info!("voting no");
                let reply = raft::VoteReply {
                    term: self.term,
                    vote_granted: false,
                };
                sender.send(reply).unwrap();
            }
            RaftEvent::AppendEntriesRequest(request, sender) => {
                info!("Got an append entries request!");
                let res = log.log_entries(request.entries, log.current_raft_index.load(std::sync::atomic::Ordering::Relaxed));
                if res.is_err() {
                    println!("Failed to append entries: {:?}", res.err().unwrap());
                    let reply = raft::AppendEntriesReply {
                        term: self.term,
                        success: false,
                    };
                    sender.send(reply).unwrap();
                    return None;
                }
                let reply = raft::AppendEntriesReply {
                    term: self.term,
                    success: true,
                };
                sender.send(reply).unwrap();
                self.last_heartbeat = Instant::now();
            }
            RaftEvent::NewEntry {
                key: _,
                value,
                sender,
            } => {
                println!("Got a new entry: {:?}", value);
                //TODO: Proxy the request to the leader
                let reply = "I am not the leader".to_string();
                let _ = sender.send(reply).unwrap();
            }
            RaftEvent::GetEntry(key, sender) => {
                //TODO: Proxy the request to the leader
                println!("Got a get entry request: {:?}", key);
                sender.send(Err(NullDbReadError::NotLeader)).unwrap();
            }
        }
        None
    }
}
