use std::{sync::Arc, time::Duration};

use flyio_rs::{
    error::ErrorCode,
    network::Network,
    request::{Extension, Payload},
    response::IntoResponse,
    routing::Router,
    serve, setup_network,
    trace::setup_with_telemetry,
    NodeId, Rpc,
};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::{
    pin,
    sync::{mpsc, oneshot, watch},
};
use tracing::{instrument, Level, Span};

use crate::{
    node::{Node, NodeState, State},
    oplog::{Operation, OperationHandler, OperationQueue},
    storage::Storage,
};

mod node;
mod oplog;
mod storage;

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let (network, rpc, messages) = setup_network().await?;
    setup_with_telemetry(format!("lin-kv-{}", network.id))?;

    let (node, state) = Node::start(network.id.clone());
    let storage = Storage::create();
    let (controller, heartbeats, votes) =
        ElectionController::create(node, rpc.clone(), network.clone());

    let (ophandler, opqueue, replication_queue) =
        OperationHandler::create(state.clone(), rpc.clone(), network.clone());

    let router = Router::new()
        .route("heartbeat", heartbeat)
        .route("campaign", campaign)
        .route("read", read)
        .route("write", write)
        .route("cas", cas)
        .route("replicate", oplog::replicate)
        .route("commit", oplog::commit)
        .layer(Extension(state.clone()))
        .layer(Extension(opqueue))
        .layer(Extension(heartbeats))
        .layer(Extension(votes))
        .layer(Extension(storage.clone()))
        .layer(Extension(replication_queue))
        .layer(Extension(network.clone()))
        .layer(Extension(rpc.clone()));

    tokio::spawn(controller.start(state.clone()));
    tokio::spawn(ophandler.start(storage));

    serve(router, rpc, messages).await?;

    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct HeartbeatRequest {
    term: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "htype", rename_all = "snake_case")]
pub enum HeartbeatResponse {
    Ok,
    NotALeader { leader: Option<NodeId>, term: u64 },
}

#[instrument(skip(beats), ret)]
async fn heartbeat(
    from: NodeId,
    Extension(mut beats): Extension<mpsc::Sender<Heartbeat>>,
    Payload(beat): Payload<HeartbeatRequest>,
) -> Payload<HeartbeatResponse> {
    let (sync, r) = oneshot::channel();

    beats
        .send(Heartbeat {
            from,
            term: beat.term,
            sync,
        })
        .await
        .unwrap();

    let r = match r.await {
        Ok(Ok(())) => HeartbeatResponse::Ok,
        Ok(Err((leader, term))) => HeartbeatResponse::NotALeader { leader, term },
        Err(_) => panic!(),
    };

    Payload(r)
}

#[derive(Debug)]
struct Heartbeat {
    from: NodeId,
    term: u64,
    sync: oneshot::Sender<Result<(), (Option<NodeId>, u64)>>,
}

struct VoteRequest {
    from: NodeId,
    term: u64,
    sync: oneshot::Sender<bool>,
}

struct ElectionController {
    heartbeats: mpsc::Receiver<Heartbeat>,
    votes: mpsc::Receiver<VoteRequest>,
    node: Node,
    rpc: Rpc,
    network: Arc<Network>,
}

impl ElectionController {
    fn heartbeat_timeout() -> Duration {
        use rand::prelude::Distribution;

        let mut rng = rand::thread_rng();
        let distr = rand::distributions::Uniform::new_inclusive(25, 75);
        Duration::from_millis(300 + distr.sample(&mut rng))
    }

    fn create(
        node: Node,
        rpc: Rpc,
        network: Arc<Network>,
    ) -> (Self, mpsc::Sender<Heartbeat>, mpsc::Sender<VoteRequest>) {
        let (txh, heartbeats) = mpsc::channel(32);
        let (txv, votes) = mpsc::channel(32);

        let s = Self {
            node,
            votes,
            heartbeats,
            rpc,
            network,
        };

        (s, txh, txv)
    }

    #[instrument(name = "election_loop", skip(self, state))]
    async fn start(mut self, mut state: State) {
        loop {
            let state: NodeState = {
                let r = state.borrow_and_update();
                (*r).clone()
            };
            match state {
                NodeState::Follower { .. } => {
                    let run_elections = self.follow().await;
                    if run_elections {
                        self.node.become_candidate();
                    }
                }
                NodeState::Leader { .. } => {
                    let continue_leading = self.lead().await;

                    if continue_leading {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                }
                NodeState::Candidate { .. } => {
                    let won = self.campaign().await;
                    if won {
                        self.node.become_leader()
                    } else {
                        self.node.become_follower(None);
                    }
                }
            }
        }
    }

    #[instrument(skip(self), fields(term = self.node.term), ret)]
    async fn follow(&mut self) -> bool {
        let heartbeat_timeout = Self::heartbeat_timeout();
        let of = self.node.state().leader();

        loop {
            tokio::select! {
                _ = tokio::time::sleep(heartbeat_timeout) => { return true },
                h = self.heartbeats.recv() => {
                    match h {
                        Some(Heartbeat { from, term, sync }) if Some(&from) == of.as_ref() && term == self.node.term => {
                            let _ = sync.send(Ok(()));
                        }
                        Some(Heartbeat { from, term, sync }) if of.is_none() && term == self.node.term => {
                            tracing::info!(%from, %term, "Got a heartbeat, starting following");
                            self.node.become_follower(Some(from));
                            let _ = sync.send(Ok(()));
                        }
                        Some(Heartbeat { from, term, sync }) if term > self.node.term => {
                            tracing::info!(%from, %term, "Got a heartbeat in greater term, becoming follower");
                            self.node.become_follower(Some(from));
                            self.node.term = term;

                            let _ = sync.send(Ok(()));
                        }
                        Some(Heartbeat { sync, .. }) => {
                            let _ = sync.send(Err((of.clone(), self.node.term)));
                        }
                        None => panic!()
                    }

                    break
                }
                Some(VoteRequest{ from, term, sync, .. }) = self.votes.recv() => {
                    if term < self.node.term {
                        let _ = sync.send(false);
                        continue
                    }

                    let _ = sync.send(self.node.cast_vote(from, term).is_ok());

                    break
                }
            }
        }

        false
    }

    #[instrument(skip(self), fields(term = self.node.term), ret)]
    async fn campaign(&mut self) -> bool {
        let term = self.node.term;
        let majority = self.network.all_nodes().len() / 2 + 1;
        let mut votes = 1usize;

        let mut responses = self.rpc.broadcast::<_, CampaignResponse>(
            "campaign",
            &self.network,
            CampaignRequest { term },
        );

        loop {
            tokio::select! {
                response = responses.next() => {
                    match response {
                        Some(Ok((from, CampaignResponse::Support))) => {
                            tracing::debug!(%from, "Got a vote");
                            votes += 1;
                        }
                        Some(Ok((_, CampaignResponse::NotInterested))) => {
                            tracing::debug!("Got not interested");
                        }
                        Some(Err(_)) => {}
                        None => break,
                    }

                    if votes >= majority {
                        tracing::info!("Reached majority, breaking the loop");
                        break;
                    }
                }
                Some(Heartbeat{ term, from, sync, .. }) = self.heartbeats.recv() => {
                    if term < self.node.term {
                        // TODO: returning OK here is not ok
                        let _ = sync.send(Ok(()));
                        continue
                    }
                    tracing::info!(%from, %term, "Got a heartbeat while campaigning, becoming follower");
                    self.node.become_follower(Some(from));
                    self.node.term = term;
                    let _ = sync.send(Ok(()));

                    return false
                }
                Some(VoteRequest{ from, term, sync, .. }) = self.votes.recv() => {
                    if term <= self.node.term {
                        let _ = sync.send(false);
                        continue
                    }

                    self.node.become_follower(None);
                    self.node.cast_vote(from, term).unwrap();

                    let _ = sync.send(true);

                    return false
                }
                else => {
                    break
                }
            }
        }
        tracing::info!(%votes, %majority, "Finishing campaign");
        return votes >= majority;
    }

    #[instrument(skip(self), fields(term = self.node.term, round), ret(level = Level::DEBUG))]
    async fn lead(&mut self) -> bool {
        assert_eq!(&*self.node.state(), &NodeState::Leader);
        // *round += 1;
        // Span::current().record("round", *round);
        //
        // if *round > 2000 {
        //     self.node.become_follower(None);
        //     return false;
        // }

        let mut responses = self.rpc.broadcast::<_, HeartbeatResponse>(
            "heartbeat",
            &self.network,
            HeartbeatRequest {
                term: self.node.term,
            },
        );

        loop {
            tokio::select! {
                response = responses.next() => {
                    let Some(response) = response else { break };

                    match response {
                        Ok((_, HeartbeatResponse::Ok)) => {}
                        Ok((_, HeartbeatResponse::NotALeader { leader, term })) => {
                            if term <= self.node.term { continue }
                            tracing::info!(?leader, %term, "I am no longer a leader");

                            return false
                        }
                        Err(_) => {}
                    }
                }
                Some(Heartbeat{ term, from, sync, .. }) = self.heartbeats.recv() => {
                    if term <= self.node.term {
                        // TODO: returning OK here is not ok
                        let _ = sync.send(Ok(()));
                        continue
                    }
                    tracing::info!(%term, %from, "Got a heartbeat from another leader, becoming a follower");
                    self.node.become_follower(Some(from));
                    self.node.term = term;
                    let _ = sync.send(Ok(()));

                    return false
                }
                Some(VoteRequest{ from, term, sync, .. }) = self.votes.recv() => {
                    if term <= self.node.term {
                        let _ = sync.send(false);
                        continue
                    }
                    tracing::info!(%from, %term, "Someone campaigning when I am a leader, casting vote and becoming follower");
                    self.node.become_follower(None);
                    self.node.cast_vote(from, term).unwrap();

                    let _ = sync.send(true);

                    return false
                }
            }
        }

        true
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct CampaignRequest {
    term: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "ctype", rename_all = "snake_case")]
enum CampaignResponse {
    Support,
    NotInterested,
}

#[instrument(skip(votes), ret)]
async fn campaign(
    from: NodeId,
    Extension(mut votes): Extension<mpsc::Sender<VoteRequest>>,
    Payload(request): Payload<CampaignRequest>,
) -> impl IntoResponse {
    let (sync, r) = oneshot::channel();

    let _ = votes
        .send(VoteRequest {
            from,
            term: request.term,
            sync,
        })
        .await
        .unwrap();

    let vote = r.await.unwrap();

    if vote {
        Payload(CampaignResponse::Support)
    } else {
        Payload(CampaignResponse::NotInterested)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ReadRequest {
    key: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ReadResponse {
    value: u64,
}

#[instrument(skip(storage), ret, err)]
async fn read(
    Extension(storage): Extension<Storage>,
    Payload(read): Payload<ReadRequest>,
) -> Result<Payload<ReadResponse>, ErrorCode> {
    Ok(Payload(ReadResponse {
        value: storage.read(read.key).ok_or(ErrorCode::KeyDoesNotExist)?,
    }))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WriteRequest {
    key: u64,
    value: u64,
}

#[instrument(skip(queue), ret)]
async fn write(Extension(queue): Extension<OperationQueue>, Payload(write): Payload<WriteRequest>) {
    let WriteRequest { key, value } = write;
    let (sync, ok) = oneshot::channel();
    let op = Operation::Write { key, value };
    queue.send((op, sync)).await.unwrap();
    let _ = ok.await;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CasRequest {
    key: u64,
    from: u64,
    to: u64,
}

#[instrument(skip(queue), ret, err)]
async fn cas(
    Extension(queue): Extension<OperationQueue>,
    Payload(cas): Payload<CasRequest>,
) -> Result<(), ErrorCode> {
    let CasRequest { key, from, to } = cas;
    let (sync, ok) = oneshot::channel();
    let op = Operation::Cas { key, from, to };
    queue.send((op, sync)).await.unwrap();
    let () = ok.await.expect("not to be dropped")?;

    Ok(())
}
