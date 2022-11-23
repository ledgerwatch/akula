#![allow(unused, unused_variables, unused_imports)]
use self::sentry::SentryService;
use super::{behaviour::BehaviourEvent, service::Libp2pService};
use crate::{
    binutil::AkulaDataDir,
    libp2p_sentry::peer_info::PeerInfoEvent,
    models::ChainConfig,
    p2p::types::PeerFilter,
    sentry::{
        devp2p::Message,
        eth::{EthMessageId, FullStatusData},
    },
};
use anyhow::anyhow;
use async_trait::async_trait;
use byteorder::{BigEndian, LittleEndian, ReadBytesExt};
use ethereum_interfaces::sentry::{
    sentry_server::Sentry, InboundMessage as ProtoInboundMessage, MessageId as ProtoMessageId,
};
use ethereum_types::H512;
use futures::{Future, FutureExt, Stream};
use hashbrown::HashSet;
use libp2p::{
    identity::{
        secp256k1::{Keypair as Secp256k1Keypair, SecretKey as Secp256k1SecretKey},
        Keypair,
    },
    swarm::SwarmEvent,
    Multiaddr, PeerId,
};
use num_traits::FromPrimitive;
use std::{io::Cursor, pin::Pin, time::Duration};
use tokio::sync::{
    broadcast::{channel as broadcast_channel, Sender as BroadcastSender},
    mpsc,
    oneshot::Sender as OneshotSender,
};
use tokio_stream::{wrappers::BroadcastStream, StreamExt};
use tracing::*;
use unsigned_varint::io::read_u64;

pub type InboundMessageStream =
    Pin<Box<dyn Stream<Item = anyhow::Result<ProtoInboundMessage, tonic::Status>> + Send + Sync>>;

pub mod sentry {
    use std::pin::Pin;

    use super::{h512_to_peer_id, Command, InboundMessageStream};
    use crate::{
        p2p::types::PeerFilter,
        sentry::{devp2p::Message, eth::FullStatusData},
    };
    use anyhow::anyhow;
    use async_trait::async_trait;
    use ethereum_interfaces::{
        sentry::{
            sentry_server::Sentry, HandShakeReply, MessagesRequest, OutboundMessageData,
            PeerByIdReply, PeerByIdRequest, PeerCountReply, PeerCountRequest, PeerEvent,
            PeerEventsRequest, PeerMinBlockRequest, PeersReply, PenalizePeerRequest,
            SendMessageByIdRequest, SendMessageByMinBlockRequest, SendMessageToRandomPeersRequest,
            SentPeers, SetStatusReply, StatusData,
        },
        types::NodeInfoReply,
    };
    use ethereum_types::H512;
    use futures::Stream;
    use hashbrown::HashSet;
    use thiserror::Error;
    use tokio::sync::{mpsc, oneshot};
    use tonic::{Request, Response};

    type Result<T> = std::result::Result<Response<T>, tonic::Status>;

    fn ok<T>(t: T) -> Result<T> {
        Ok(Response::new(t))
    }

    #[inline]
    fn into_tonic(e: impl std::fmt::Display) -> tonic::Status {
        tonic::Status::internal(e.to_string())
    }

    trait SendMsgRequest {
        fn dispatch(self) -> anyhow::Result<(Message, PeerFilter)>;
    }

    impl SendMsgRequest for SendMessageByIdRequest {
        fn dispatch(self) -> anyhow::Result<(Message, PeerFilter)> {
            // FIXME: implement libp2p peer ids.
            let SendMessageByIdRequest { data, .. } = self;
            Ok((
                data.ok_or_else(|| anyhow!("No data to send"))?.try_into()?,
                PeerFilter::All,
            ))
        }
    }

    impl SendMsgRequest for SendMessageByMinBlockRequest {
        fn dispatch(self) -> anyhow::Result<(Message, PeerFilter)> {
            let SendMessageByMinBlockRequest {
                data, min_block, ..
            } = self;
            Ok((
                data.ok_or_else(|| anyhow!("No data to send"))?.try_into()?,
                PeerFilter::MinBlock(min_block),
            ))
        }
    }

    impl SendMsgRequest for SendMessageToRandomPeersRequest {
        fn dispatch(self) -> anyhow::Result<(Message, PeerFilter)> {
            let SendMessageToRandomPeersRequest {
                data, max_peers, ..
            } = self;
            Ok((
                data.ok_or_else(|| anyhow!("No data to send"))?.try_into()?,
                PeerFilter::Random(max_peers),
            ))
        }
    }

    impl SendMsgRequest for OutboundMessageData {
        fn dispatch(self) -> anyhow::Result<(Message, PeerFilter)> {
            Ok((self.try_into()?, PeerFilter::All))
        }
    }

    impl<T: SendMsgRequest> SendMsgRequest for Request<T> {
        fn dispatch(self) -> anyhow::Result<(Message, PeerFilter)> {
            self.into_inner().dispatch()
        }
    }

    // fn preprocess_msg_request(request: Request<)

    pub struct SentryService {
        command_tx: mpsc::Sender<Command>,
    }

    impl SentryService {
        pub fn new(command_tx: mpsc::Sender<Command>) -> Self {
            Self { command_tx }
        }
    }

    #[async_trait]
    impl Sentry for SentryService {
        async fn set_status(&self, r: Request<StatusData>) -> Result<SetStatusReply> {
            self.command_tx
                .send(Command::SetStatus(
                    FullStatusData::try_from(r.into_inner()).map_err(into_tonic)?,
                ))
                .await
                .map_err(into_tonic)?;
            ok(SetStatusReply {})
        }

        async fn penalize_peer(&self, r: Request<PenalizePeerRequest>) -> Result<()> {
            let (tx, rx) = oneshot::channel();
            self.command_tx
                .send(Command::PenalizePeer(
                    r.into_inner()
                        .peer_id
                        .map(h512_to_peer_id)
                        .ok_or_else(|| tonic::Status::invalid_argument("no peer_id specified"))?,
                    tx,
                ))
                .await
                .map_err(into_tonic)?;
            ok(rx.await.map_err(into_tonic)?.map_err(into_tonic)?)
        }

        async fn peer_min_block(&self, _: Request<PeerMinBlockRequest>) -> Result<()> {
            todo!()
        }

        async fn hand_shake(&self, _: Request<()>) -> Result<HandShakeReply> {
            Ok(Response::new(HandShakeReply {
                protocol: ethereum_interfaces::sentry::Protocol::Eth66 as i32,
            }))
        }

        async fn send_message_by_min_block(
            &self,
            r: Request<SendMessageByMinBlockRequest>,
        ) -> Result<SentPeers> {
            let (data, filter) = r.dispatch().map_err(into_tonic)?;
            self.command_tx
                .send(Command::SendMessage(data, filter))
                .await
                .map_err(into_tonic)?;
            ok(SentPeers { peers: vec![] })
        }

        async fn send_message_by_id(
            &self,
            r: Request<SendMessageByIdRequest>,
        ) -> Result<SentPeers> {
            let (data, filter) = r.dispatch().map_err(into_tonic)?;
            self.command_tx
                .send(Command::SendMessage(data, filter))
                .await
                .map_err(into_tonic)?;
            ok(SentPeers { peers: vec![] })
        }

        async fn send_message_to_random_peers(
            &self,
            r: Request<SendMessageToRandomPeersRequest>,
        ) -> Result<SentPeers> {
            let (data, filter) = r.dispatch().map_err(into_tonic)?;
            self.command_tx
                .send(Command::SendMessage(data, filter))
                .await
                .map_err(into_tonic)?;
            ok(SentPeers { peers: vec![] })
        }

        async fn send_message_to_all(&self, r: Request<OutboundMessageData>) -> Result<SentPeers> {
            let (data, filter) = r.dispatch().map_err(into_tonic)?;
            self.command_tx
                .send(Command::SendMessage(data, filter))
                .await
                .map_err(into_tonic)?;
            Ok(Response::new(SentPeers { peers: vec![] }))
        }

        type MessagesStream = InboundMessageStream;
        async fn messages(&self, r: Request<MessagesRequest>) -> Result<Self::MessagesStream> {
            let ids = r.into_inner().ids.into_iter().collect::<HashSet<_>>();
            let (tx, rx) = oneshot::channel();
            self.command_tx
                .send(Command::Subscribe(ids, tx))
                .await
                .map_err(into_tonic)?;
            Ok(Response::new(rx.await.map_err(into_tonic)?))
        }

        async fn peers(&self, _: Request<()>) -> Result<PeersReply> {
            todo!()
        }

        async fn peer_count(&self, _: Request<PeerCountRequest>) -> Result<PeerCountReply> {
            // FIXME: implement peer count.
            Ok(Response::new(PeerCountReply { count: 100 }))
        }

        async fn peer_by_id(&self, _: Request<PeerByIdRequest>) -> Result<PeerByIdReply> {
            todo!()
        }

        type PeerEventsStream =
            Pin<Box<dyn Stream<Item = anyhow::Result<PeerEvent, tonic::Status>> + Send + Sync>>;

        async fn peer_events(
            &self,
            _: Request<PeerEventsRequest>,
        ) -> Result<Self::PeerEventsStream> {
            todo!()
        }

        async fn node_info(&self, _: Request<()>) -> Result<NodeInfoReply> {
            todo!()
        }
    }
}

pub enum Command {
    SetStatus(FullStatusData),
    SendMessage(Message, PeerFilter),
    PenalizePeer(PeerId, OneshotSender<anyhow::Result<()>>),
    Subscribe(HashSet<i32>, OneshotSender<InboundMessageStream>),
}

pub struct Orchestrator {
    libp2p: Libp2pService,

    data_sender: BroadcastSender<ProtoInboundMessage>,
    command_rx: mpsc::Receiver<Command>,
}

pub(super) fn peer_id_to_h512(peer_id: &PeerId) -> impl Into<ethereum_interfaces::types::H512> {
    let mut bytes = [0u8; 64];
    let peer_id = peer_id.to_bytes();
    bytes[63] = peer_id.len() as u8;
    bytes[..peer_id.len()].copy_from_slice(&peer_id);
    H512(bytes)
}
pub(super) fn h512_to_peer_id(peer_id: impl Into<ethereum_types::H512>) -> PeerId {
    let bytes: [u8; 64] = peer_id.into().0;
    PeerId::from_bytes(&bytes[..bytes[63] as usize]).expect("PeerId bytes are always valid")
}

impl Orchestrator {
    pub fn new(
        addr: Multiaddr,
        bootnodes: Vec<Multiaddr>,
        db_path: AkulaDataDir,
    ) -> anyhow::Result<(Self, SentryService)> {
        let keypair = {
            let secret_key_path = db_path.libp2p();
            let k;
            match std::fs::read_to_string(&secret_key_path) {
                Ok(nodekey) => {
                    let secret_key = Secp256k1Keypair::from(Secp256k1SecretKey::from_bytes(
                        &mut hex::decode(nodekey)?[..],
                    )?);
                    info!(
                        "Loaded node key: {}",
                        hex::encode(&secret_key.secret().to_bytes())
                    );

                    k = Keypair::Secp256k1(secret_key);
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    k = Keypair::generate_secp256k1();
                    if let Keypair::Secp256k1(secp256k1) = &k {
                        let key = hex::encode(secp256k1.secret().to_bytes());
                        info!("Generated new node key: {}", &key);
                        std::fs::write(&secret_key_path, key)?;
                    }
                }
                Err(e) => return Err(e.into()),
            }
            k
        };

        let (command_tx, command_rx) = mpsc::channel(1024);
        let (data_sender, _) = broadcast_channel(1024);
        Ok((
            Self {
                libp2p: Libp2pService::new(keypair, addr, bootnodes)?,
                data_sender,
                command_rx,
            },
            SentryService::new(command_tx),
        ))
    }

    pub async fn run(&mut self) {
        let status = self.libp2p.behaviour().protocol.status.clone();
        let mut sleep = tokio::time::interval(Duration::from_secs(1));

        loop {
            tokio::select! {
                Some(e) = self.libp2p.next() => {
                    match e {
                        SwarmEvent::Behaviour(event) => match event {
                            BehaviourEvent::Discovery(_discovery_event) => {}
                            BehaviourEvent::PeerInfo(peer_info_event) => match peer_info_event {
                                PeerInfoEvent::PeerIdentified { peer_id, addresses } => {
                                    debug!("Peer identified: {}, {:?}", peer_id, addresses);
                                }
                                PeerInfoEvent::PeerInfoUpdated { peer_id } => {
                                    debug!("Peer info updated: {}", peer_id);
                                }
                            },
                            BehaviourEvent::Protocol((peer_id, Message { id, data })) => {
                                if let Some(message_id) = EthMessageId::from_usize(id).map(ProtoMessageId::from)
                                {
                                    let msg = ProtoInboundMessage {
                                        id: message_id as i32,
                                        data,
                                        peer_id: Some(H512::default().into()),
                                    };
                                    if cfg!(debug_assertions) {
                                        assert!(crate::p2p::types::InboundMessage::new(msg.clone(), 0).is_ok());
                                    }
                                    let _ = self.data_sender.send(msg);
                                }
                            }
                        },
                        other => println!("other: {:?}", other),
                    }
                },
                Some(cmd) = self.command_rx.recv() => {
                    match cmd {
                        Command::SetStatus(status_data) => {
                            *status.write() = Some(status_data);
                        }
                        Command::SendMessage(msg, pred) => {
                            self.libp2p.send_message(msg, pred);
                        }
                        Command::PenalizePeer(peer_id, result_tx) => {
                            let result = self
                                .libp2p
                                .disconnect_peer_id(peer_id)
                                .map_err(|_| anyhow!("Failed to penalize peer"));
                            if result.is_err() {
                                debug!("Failed to disconnect peer {}, it's not connected", peer_id);
                            };
                            result_tx.send(result);
                        }
                        Command::Subscribe(ids, result_tx) => {
                            let stream = Box::pin(
                                BroadcastStream::new(self.data_sender.subscribe()).map(|m| {println!("{m:?}"); m })
                                    .filter_map(Result::ok)
                                    .filter(move |m| ids.is_empty() || ids.contains(&m.id))
                                    .map(Ok),
                            );
                            if result_tx.send(stream).is_err() {
                                debug!("Failed to send subscription stream");
                            }
                        }
                    }
                }
                _ = sleep.tick() => {
                    // for bootnode in self.libp2p.bootnodes.clone() {
                    //     self.libp2p.dial(bootnode);

                    // }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use ethereum_interfaces::sentry::{MessagesRequest, OutboundMessageData, StatusData};
    use ethereum_types::H256;
    use fastrlp::Encodable;
    use libp2p::multiaddr::Protocol;
    use tonic::Request;

    #[test]
    fn test_convert_peer_id() {
        let peer_id = PeerId::random();
        let h512 = peer_id_to_h512(&peer_id).into();
        let peer_id2 = h512_to_peer_id(h512);
        assert_eq!(peer_id, peer_id2);
    }

    fn build_swarm(addr: Multiaddr, bootnodes: Vec<Multiaddr>) -> (Orchestrator, SentryService) {
        let keypair = Keypair::generate_secp256k1();
        let (orchestrator, sentry_service) =
            Orchestrator::new(addr, bootnodes, AkulaDataDir::temp()).unwrap();
        (orchestrator, sentry_service)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_orchestrator() -> anyhow::Result<()> {
        tracing_subscriber::fmt::init();
        let mut addr1: Multiaddr = "/ip4/127.0.0.1".parse().unwrap();
        addr1.push(Protocol::Tcp(30304));
        let (mut orch, sentry_service1) = build_swarm(addr1.clone(), vec![]);
        let peer_id1 = orch.libp2p.keypair.public().to_peer_id().clone();

        tokio::spawn(async move { orch.run().await });

        let mut addr2: Multiaddr = "/ip4/127.0.0.1".parse().unwrap();
        addr2.push(Protocol::Tcp(30305));

        let (mut orch, sentry_service2) = build_swarm(addr2, vec![addr1]);
        let peer_id2 = orch.libp2p.keypair.public().to_peer_id().clone();

        tokio::spawn(async move { orch.run().await });

        let mut stream2 = sentry_service2
            .messages(Request::new(MessagesRequest { ids: vec![] }))
            .await?
            .into_inner();
        let mut stream1 = sentry_service1
            .messages(Request::new(MessagesRequest { ids: vec![] }))
            .await?
            .into_inner();

        let mut sleep = tokio::time::interval(Duration::from_secs(1));
        let m = {
            let tmp =
                crate::p2p::types::Message::GetBlockHeaders(crate::p2p::types::GetBlockHeaders {
                    request_id: 0,
                    params: crate::p2p::types::HeaderRequest::default().into(),
                });
            let mut buf = BytesMut::new();
            tmp.encode(&mut buf);
            OutboundMessageData {
                id: ProtoMessageId::from(tmp.id()) as i32,
                data: buf.freeze(),
            }
        };

        let status_data = StatusData {
            network_id: 1,
            total_difficulty: Some(H256::default().into()),
            best_hash: Some(H256::default().into()),
            fork_data: Some(ethereum_interfaces::sentry::Forks {
                genesis: Some(H256::default().into()),
                forks: vec![],
            }),
            max_block: 0,
        };

        sentry_service2
            .set_status(Request::new(status_data.clone()))
            .await?;

        sentry_service1
            .set_status(Request::new(status_data))
            .await?;

        let (mut sent, mut received1, mut received2) = (0, 0, 0);

        loop {
            sentry_service2
                .send_message_to_all(Request::new(m.clone()))
                .await?;
            sentry_service1
                .send_message_to_all(Request::new(m.clone()))
                .await?;
            sent += 1;

            if let Ok(Some(Ok(msg))) =
                tokio::time::timeout(Duration::from_secs(1), stream1.next()).await
            {
                println!("Stream1: {:?}", msg);
                received1 += 1;
            }
            if let Ok(Some(Ok(msg))) =
                tokio::time::timeout(Duration::from_secs(1), stream2.next()).await
            {
                println!("Stream2: {:?}", msg);
                received2 += 1;
            }

            if received1 == 0 && received2 == 0 {
                sent -= 1;
            } else if sent == received1 && sent == received2 {
                break;
            }
        }

        sentry_service1
            .penalize_peer(Request::new(
                ethereum_interfaces::sentry::PenalizePeerRequest {
                    peer_id: Some(peer_id_to_h512(&peer_id2).into()),
                    penalty: 0,
                },
            ))
            .await?;

        Ok(())
    }
}
