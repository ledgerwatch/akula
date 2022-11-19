use crate::sentry::eth::{FullStatusData, StatusMessage};
use async_trait::async_trait;
use bytes::BytesMut;
use fastrlp::{Decodable, Encodable};
use futures::{AsyncReadExt, AsyncWriteExt};
use hashbrown::HashSet;
use libp2p::{
    request_response::{
        handler::RequestResponseHandler, ProtocolSupport, RequestResponse, RequestResponseCodec,
        RequestResponseEvent, RequestResponseMessage,
    },
    swarm::{CloseConnection, NetworkBehaviour, NetworkBehaviourAction},
    Multiaddr, PeerId,
};
use parking_lot::RwLock;
use std::{collections::VecDeque, sync::Arc, task::Poll};
use tracing::*;

pub const PROTOCOL_NAME: &[u8] = "/sentry/1.0.0".as_bytes();

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtocolEvent {
    Status,
    Message(crate::sentry::devp2p::Message),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtocolResponse {
    Ok,
    Status(StatusMessage),
}

pub struct ProtocolBehaviour {
    status: Arc<RwLock<Option<FullStatusData>>>,

    request_response: RequestResponse<ProtocolCodec>,
    pending_events: VecDeque<(PeerId, crate::sentry::devp2p::Message)>,
    valid_peers: HashSet<PeerId>,
}

impl ProtocolBehaviour {
    pub fn new() -> Self {
        Self {
            status: Default::default(),
            request_response: RequestResponse::new(
                ProtocolCodec(),
                std::iter::once((PROTOCOL_NAME, ProtocolSupport::Full)),
                Default::default(),
            ),
            pending_events: Default::default(),
            valid_peers: Default::default(),
        }
    }

    pub fn send_message(&mut self, peer: &PeerId, msg: crate::sentry::devp2p::Message) {
        self.request_response
            .send_request(peer, ProtocolEvent::Message(msg));
    }
}

#[derive(Clone, Copy)]
pub struct ProtocolCodec();

#[async_trait]
impl RequestResponseCodec for ProtocolCodec {
    type Protocol = &'static [u8];
    type Request = ProtocolEvent;
    type Response = ProtocolResponse;

    async fn read_request<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
    ) -> std::io::Result<Self::Request>
    where
        T: futures::io::AsyncRead + Unpin + Send,
    {
        let mut buf = [0u8; 1];
        io.read_exact(&mut buf).await?;
        match buf[0] {
            0 => Ok(ProtocolEvent::Status),
            1 => {
                let msg_id = {
                    let mut buf = [0u8; 1];
                    io.read_exact(&mut buf).await?;
                    u8::from_be_bytes(buf) as usize
                };

                let msg_len = {
                    let mut buf = [0u8; 4];
                    io.read_exact(&mut buf).await?;
                    u32::from_be_bytes(buf) as usize
                };

                let data = {
                    let mut buf = vec![0u8; msg_len];
                    io.read_exact(&mut buf).await?;
                    buf.into()
                };
                Ok(ProtocolEvent::Message(crate::sentry::devp2p::Message {
                    id: msg_id,
                    data,
                }))
            }
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Invalid message",
            )),
        }
    }

    async fn read_response<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
    ) -> std::io::Result<Self::Response>
    where
        T: futures::io::AsyncRead + Unpin + Send,
    {
        let mut buf = [0u8; 1];
        io.read_exact(&mut buf).await?;

        match buf[0] {
            0 => Ok(ProtocolResponse::Ok),
            1 => {
                let len = {
                    let mut buf = [0u8; 4];
                    io.read_exact(&mut buf).await?;
                    u32::from_be_bytes(buf) as usize
                };
                let status = {
                    let mut buf = vec![0u8; len];
                    io.read_exact(&mut buf).await?;
                    StatusMessage::decode(&mut &buf[..]).map_err(|e| {
                        std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())
                    })?
                };
                Ok(ProtocolResponse::Status(status))
            }
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Invalid message",
            )),
        }
    }

    async fn write_request<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
        request: Self::Request,
    ) -> std::io::Result<()>
    where
        T: futures::io::AsyncWrite + Unpin + Send,
    {
        match request {
            ProtocolEvent::Status => {
                io.write_all(&[0]).await?;
            }
            ProtocolEvent::Message(msg) => {
                io.write_all(&[1]).await?;
                io.write_all(&(msg.id as u8).to_be_bytes()).await?;
                io.write_all(&(msg.data.len() as u32).to_be_bytes()).await?;
                io.write_all(&msg.data).await?;
            }
        }
        Ok(())
    }

    async fn write_response<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
        response: Self::Response,
    ) -> std::io::Result<()>
    where
        T: futures::io::AsyncWrite + Unpin + Send,
    {
        match response {
            ProtocolResponse::Ok => {
                io.write_all(&[0]).await?;
            }
            ProtocolResponse::Status(status) => {
                io.write_all(&[1]).await?;

                let status_encoded = {
                    let mut out = BytesMut::new();
                    status.encode(&mut out);
                    out
                };

                io.write_all(&(status_encoded.len() as u32).to_be_bytes())
                    .await?;
                io.write_all(&status_encoded).await?;
            }
        };
        Ok(())
    }
}

impl NetworkBehaviour for ProtocolBehaviour {
    type ConnectionHandler = RequestResponseHandler<ProtocolCodec>;
    type OutEvent = (PeerId, crate::sentry::devp2p::Message);

    fn new_handler(&mut self) -> Self::ConnectionHandler {
        self.request_response.new_handler()
    }

    fn addresses_of_peer(&mut self, peer: &PeerId) -> Vec<Multiaddr> {
        self.request_response.addresses_of_peer(peer)
    }

    fn inject_address_change(
        &mut self,
        peer: &PeerId,
        conn: &libp2p::core::connection::ConnectionId,
        old: &libp2p::core::ConnectedPoint,
        new: &libp2p::core::ConnectedPoint,
    ) {
        self.request_response
            .inject_address_change(peer, conn, old, new)
    }

    fn inject_connection_established(
        &mut self,
        peer_id: &PeerId,
        connection_id: &libp2p::core::connection::ConnectionId,
        endpoint: &libp2p::core::ConnectedPoint,
        failed_addresses: Option<&Vec<libp2p::Multiaddr>>,
        other_established: usize,
    ) {
        self.request_response.inject_connection_established(
            peer_id,
            connection_id,
            endpoint,
            failed_addresses,
            other_established,
        );

        self.request_response
            .send_request(peer_id, ProtocolEvent::Status);
    }

    fn inject_connection_closed(
        &mut self,
        peer_id: &PeerId,
        connection_id: &libp2p::core::connection::ConnectionId,
        endpoint: &libp2p::core::ConnectedPoint,
        handler: <Self::ConnectionHandler as libp2p::swarm::IntoConnectionHandler>::Handler,
        remaining_established: usize,
    ) {
        self.request_response.inject_connection_closed(
            peer_id,
            connection_id,
            endpoint,
            handler,
            remaining_established,
        );
    }

    fn inject_dial_failure(
        &mut self,
        peer_id: Option<PeerId>,
        handler: Self::ConnectionHandler,
        error: &libp2p::swarm::DialError,
    ) {
        self.request_response
            .inject_dial_failure(peer_id, handler, error)
    }

    fn inject_event(
        &mut self,
        peer_id: PeerId,
        connection: libp2p::core::connection::ConnectionId,
        event: <<Self::ConnectionHandler as libp2p::swarm::IntoConnectionHandler>::Handler as libp2p::swarm::ConnectionHandler>::OutEvent,
    ) {
        self.request_response
            .inject_event(peer_id, connection, event)
    }

    fn poll(
        &mut self,
        cx: &mut std::task::Context<'_>,
        params: &mut impl libp2p::swarm::PollParameters,
    ) -> Poll<NetworkBehaviourAction<Self::OutEvent, Self::ConnectionHandler>> {
        if let Some(event) = self.pending_events.pop_front() {
            return Poll::Ready(NetworkBehaviourAction::GenerateEvent(event));
        };

        match self.request_response.poll(cx, params) {
            Poll::Ready(e) => match e {
                NetworkBehaviourAction::GenerateEvent(e) => match e {
                    RequestResponseEvent::Message { peer, message } => match message {
                        RequestResponseMessage::Request {
                            request, channel, ..
                        } => match request {
                            ProtocolEvent::Status => {
                                if let Some(FullStatusData {
                                    status,
                                    fork_filter,
                                }) = &*self.status.read()
                                {
                                    let status_message = StatusMessage {
                                        protocol_version: 17,
                                        network_id: status.network_id,
                                        total_difficulty: status.total_difficulty,
                                        best_hash: status.best_hash,
                                        genesis_hash: status.fork_data.genesis,
                                        fork_id: fork_filter.current(),
                                    };

                                    self.request_response
                                        .send_response(
                                            channel,
                                            ProtocolResponse::Status(status_message),
                                        )
                                        .unwrap();
                                } else {
                                    self.request_response
                                        .send_response(channel, ProtocolResponse::Ok)
                                        .unwrap();
                                }
                            }
                            ProtocolEvent::Message(msg) => {
                                self.request_response
                                    .send_response(channel, ProtocolResponse::Ok)
                                    .unwrap();

                                if self.valid_peers.contains(&peer) {
                                    self.pending_events.push_back((peer, msg));
                                }
                            }
                        },
                        RequestResponseMessage::Response {
                            response: ProtocolResponse::Status(StatusMessage { fork_id, .. }),
                            ..
                        } => {
                            if let Some(FullStatusData { fork_filter, .. }) = &*self.status.read() {
                                if fork_filter.validate(fork_id).is_err() {
                                    return Poll::Ready(NetworkBehaviourAction::CloseConnection {
                                        peer_id: peer,
                                        connection: CloseConnection::All,
                                    });
                                };

                                debug!("Validated: peer={}", peer);
                                self.valid_peers.insert(peer);
                            }
                        }
                        _ => (),
                    },
                    other => debug!("RequestResponseEvent: {:?}", other),
                },
                NetworkBehaviourAction::Dial { opts, handler } => {
                    return Poll::Ready(NetworkBehaviourAction::Dial { opts, handler })
                }
                NetworkBehaviourAction::NotifyHandler {
                    peer_id,
                    handler,
                    event,
                } => {
                    return Poll::Ready(NetworkBehaviourAction::NotifyHandler {
                        peer_id,
                        handler,
                        event,
                    })
                }
                NetworkBehaviourAction::ReportObservedAddr { address, score } => {
                    return Poll::Ready(NetworkBehaviourAction::ReportObservedAddr {
                        address,
                        score,
                    })
                }
                NetworkBehaviourAction::CloseConnection {
                    peer_id,
                    connection,
                } => {
                    return Poll::Ready(NetworkBehaviourAction::CloseConnection {
                        peer_id,
                        connection,
                    })
                }
            },
            Poll::Pending => return Poll::Pending,
        }

        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ethereum_forkid::{ForkFilter, ForkHash, ForkId};
    use ethereum_types::H256;
    use ethnum::U256;
    use libp2p::{
        identity::Keypair,
        noise,
        swarm::{SwarmBuilder, SwarmEvent},
        yamux, Swarm, Transport,
    };
    use rand::Rng;
    use tokio_stream::StreamExt;

    fn build_swarm(
        bootnodes: Vec<Multiaddr>,
    ) -> (
        Swarm<ProtocolBehaviour>,
        Arc<RwLock<Option<FullStatusData>>>,
        Multiaddr,
        PeerId,
    ) {
        let keypair = Keypair::generate_secp256k1();

        let noise_keys = noise::Keypair::<noise::X25519Spec>::new()
            .into_authentic(&keypair)
            .unwrap();

        let transport = libp2p::core::transport::MemoryTransport::new()
            .upgrade(libp2p::core::upgrade::Version::V1)
            .authenticate(noise::NoiseConfig::xx(noise_keys).into_authenticated())
            .multiplex(yamux::YamuxConfig::default())
            .boxed();

        let behaviour = ProtocolBehaviour::new();
        let status = behaviour.status.clone();
        let listen_addr: Multiaddr =
            libp2p::multiaddr::Protocol::Memory(rand::thread_rng().gen()).into();

        let mut swarm = SwarmBuilder::new(transport, behaviour, keypair.public().to_peer_id())
            .executor(Box::new(|f| {
                tokio::spawn(f);
            }))
            .build();
        swarm.listen_on(listen_addr.clone()).unwrap();

        for bootnode in bootnodes {
            Swarm::dial(&mut swarm, bootnode).unwrap();
        }

        (
            swarm,
            status,
            listen_addr,
            PeerId::from_public_key(&keypair.public()),
        )
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_protocol_behaviour() -> anyhow::Result<()> {
        let status = FullStatusData {
            status: crate::sentry::eth::StatusData {
                network_id: 1,
                total_difficulty: 1u8.into(),
                best_hash: H256::random(),
                fork_data: crate::sentry::eth::Forks {
                    genesis: Default::default(),
                    forks: [].into(),
                },
            },
            fork_filter: ForkFilter::new(0, Default::default(), vec![]),
        };

        let (mut swarm1, status1, listen_addr1, peer_id1) = build_swarm(vec![]);
        let (mut swarm2, status2, _, peer_id2) =
            build_swarm(vec![
                format!("{}/p2p/{}", listen_addr1.clone(), peer_id1).parse()?
            ]);
        *status1.write() = Some(status.clone());
        *status2.write() = Some(status);

        loop {
            tokio::select! {
                Some(event) = swarm1.next() => {
                    println!("swarm1: {:#?}", event);
                },
                Some(event) = swarm2.next() => {
                    println!("swarm2: {:#?}", event);
                },
                _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => { break; },
            }
        }

        swarm1.behaviour_mut().send_message(
            &peer_id2,
            crate::sentry::devp2p::Message {
                id: 0,
                data: vec![1, 2, 3].into(),
            },
        );

        loop {
            tokio::select! {
                Some(_) = swarm1.next() => {},
                Some(SwarmEvent::Behaviour((peer, msg))) = swarm2.next() => {
                    assert_eq!(peer, peer_id1);
                    assert_eq!(msg, crate::sentry::devp2p::Message {
                        id: 0,
                        data: vec![1, 2, 3].into(),
                    });
                    break;
                }
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_protocol_codec() {
        let mut codec = ProtocolCodec();

        let mut buf = Vec::new();
        codec
            .write_request(&PROTOCOL_NAME, &mut buf, ProtocolEvent::Status)
            .await
            .unwrap();
        assert_eq!(buf, vec![0]);
        assert_eq!(
            codec
                .read_request(&PROTOCOL_NAME, &mut &buf[..])
                .await
                .unwrap(),
            ProtocolEvent::Status
        );
        buf.clear();

        codec
            .write_request(
                &PROTOCOL_NAME,
                &mut buf,
                ProtocolEvent::Message(crate::sentry::devp2p::Message {
                    id: 1,
                    data: vec![1, 2, 3].into(),
                }),
            )
            .await
            .unwrap();

        assert_eq!(buf, vec![1, 1, 0, 0, 0, 3, 1, 2, 3]);

        assert_eq!(
            codec
                .read_request(&PROTOCOL_NAME, &mut &buf[..])
                .await
                .unwrap(),
            ProtocolEvent::Message(crate::sentry::devp2p::Message {
                id: 1,
                data: vec![1, 2, 3].into(),
            })
        );

        buf.clear();

        codec
            .write_response(&PROTOCOL_NAME, &mut buf, ProtocolResponse::Ok)
            .await
            .unwrap();
        assert_eq!(buf, vec![0]);

        assert_eq!(
            codec
                .read_response(&PROTOCOL_NAME, &mut &buf[..])
                .await
                .unwrap(),
            ProtocolResponse::Ok
        );

        buf.clear();

        let status = ProtocolResponse::Status(StatusMessage {
            protocol_version: 66,
            network_id: 1,
            total_difficulty: U256::from(0u64),
            best_hash: H256::from_low_u64_be(0),
            genesis_hash: H256::from_low_u64_be(0),
            fork_id: ForkId {
                hash: ForkHash([0; 4]),
                next: 0,
            },
        });
        codec
            .write_response(&PROTOCOL_NAME, &mut buf, status.clone())
            .await
            .unwrap();

        assert_eq!(
            codec
                .read_response(&PROTOCOL_NAME, &mut &buf[..])
                .await
                .unwrap(),
            status
        );
    }
}
