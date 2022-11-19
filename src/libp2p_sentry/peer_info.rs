use hashbrown::HashSet;
use hashlink::LruCache;
use libp2p::{
    core::{either::EitherOutput, transport::ListenerId, ConnectedPoint},
    identify::{Identify, IdentifyConfig, IdentifyInfo},
    identity::PublicKey,
    ping::{Ping, PingConfig, PingEvent, PingSuccess},
    swarm::{
        IntoConnectionHandler, IntoConnectionHandlerSelect, NetworkBehaviour,
        NetworkBehaviourAction,
    },
    Multiaddr, PeerId,
};
use std::{task::Poll, time::Duration};
use tracing::*;

const PEER_IDENTIFY_INTERVAL: Duration = Duration::from_secs(5);
const PING_INTERVAL: Duration = Duration::from_secs(3);

const MAX_IDENTIFY_ADDRESSES: usize = 10;
const PEER_INFO_CACHE_SIZE: usize = 128;

#[derive(Debug, Clone)]
pub enum PeerInfoEvent {
    PeerIdentified {
        peer_id: PeerId,
        addresses: Vec<Multiaddr>,
    },
    PeerInfoUpdated {
        peer_id: PeerId,
    },
}

#[derive(Debug, Clone)]
pub struct PeerInfo {
    pub addresses: HashSet<Multiaddr>,
    pub client_version: Option<String>,
    pub endpoint: ConnectedPoint,
    pub latest_ping: Option<Duration>,
}

impl PeerInfo {
    pub fn new(endpoint: ConnectedPoint) -> Self {
        Self {
            addresses: Default::default(),
            client_version: None,
            endpoint,
            latest_ping: None,
        }
    }
}

pub struct PeerInfoBehaviour {
    ping: Ping,
    identify: Identify,
    peers: LruCache<PeerId, PeerInfo>,
}

impl PeerInfoBehaviour {
    pub fn new(local_public_key: PublicKey) -> Self {
        let identify = Identify::new(
            IdentifyConfig::new("/akula/1.0".to_string(), local_public_key)
                .with_interval(PEER_IDENTIFY_INTERVAL),
        );
        let ping = Ping::new(PingConfig::new().with_interval(PING_INTERVAL));

        Self {
            ping,
            identify,
            peers: LruCache::new(PEER_INFO_CACHE_SIZE),
        }
    }

    pub fn peers(&self) -> &LruCache<PeerId, PeerInfo> {
        &self.peers
    }

    pub fn get_peer_info(&mut self, peer_id: &PeerId) -> Option<&PeerInfo> {
        self.peers.get(peer_id)
    }

    fn insert_peer_addresses(&mut self, peer_id: &PeerId, addresses: Vec<Multiaddr>) {
        if let Some(peer_info) = self.peers.get_mut(peer_id) {
            peer_info.addresses.extend(addresses);
        }
    }

    fn insert_client_version(&mut self, peer_id: &PeerId, client_version: String) {
        if let Some(peer_info) = self.peers.get_mut(peer_id) {
            peer_info.client_version = Some(client_version);
        }
    }

    fn insert_latest_ping(&mut self, peer_id: &PeerId, latest_ping: Duration) {
        if let Some(peer_info) = self.peers.get_mut(peer_id) {
            peer_info.latest_ping = Some(latest_ping);
        }
    }
}

impl NetworkBehaviour for PeerInfoBehaviour {
    type ConnectionHandler = IntoConnectionHandlerSelect<
        <Ping as NetworkBehaviour>::ConnectionHandler,
        <Identify as NetworkBehaviour>::ConnectionHandler,
    >;
    type OutEvent = PeerInfoEvent;

    fn new_handler(&mut self) -> Self::ConnectionHandler {
        IntoConnectionHandler::select(self.ping.new_handler(), self.identify.new_handler())
    }

    fn addresses_of_peer(&mut self, peer_id: &PeerId) -> Vec<Multiaddr> {
        let mut addresses = HashSet::new();
        addresses.extend(self.ping.addresses_of_peer(peer_id));
        addresses.extend(self.identify.addresses_of_peer(peer_id));
        addresses.into_iter().collect()
    }

    fn inject_connection_established(
        &mut self,
        peer_id: &PeerId,
        connection_id: &libp2p::core::connection::ConnectionId,
        endpoint: &ConnectedPoint,
        failed_addresses: Option<&Vec<Multiaddr>>,
        other_established: usize,
    ) {
        self.ping.inject_connection_established(
            peer_id,
            connection_id,
            endpoint,
            failed_addresses,
            other_established,
        );
        self.identify.inject_connection_established(
            peer_id,
            connection_id,
            endpoint,
            failed_addresses,
            other_established,
        );
        let addresses = self
            .addresses_of_peer(peer_id)
            .into_iter()
            .collect::<HashSet<_>>();
        let endpoint = endpoint.clone();

        match self.peers.entry(*peer_id) {
            hashlink::lru_cache::Entry::Occupied(mut e) => {
                let mut_ref = e.get_mut();
                mut_ref.addresses.extend(addresses);
                mut_ref.endpoint = endpoint;
            }
            hashlink::lru_cache::Entry::Vacant(e) => {
                e.insert(PeerInfo {
                    addresses,
                    client_version: None,
                    endpoint,
                    latest_ping: None,
                });
            }
        }
    }

    fn inject_connection_closed(
        &mut self,
        peer_id: &PeerId,
        connection_id: &libp2p::core::connection::ConnectionId,
        endpoint: &ConnectedPoint,
        handler: <Self::ConnectionHandler as IntoConnectionHandler>::Handler,
        remaining_established: usize,
    ) {
        let (ping_handler, identify_handler) = handler.into_inner();
        self.identify.inject_connection_closed(
            peer_id,
            connection_id,
            endpoint,
            identify_handler,
            remaining_established,
        );
        self.ping.inject_connection_closed(
            peer_id,
            connection_id,
            endpoint,
            ping_handler,
            remaining_established,
        );
    }

    fn inject_event(
        &mut self,
        peer_id: PeerId,
        connection: libp2p::core::connection::ConnectionId,
        event: <<Self::ConnectionHandler as IntoConnectionHandler>::Handler as libp2p::swarm::ConnectionHandler>::OutEvent,
    ) {
        match event {
            EitherOutput::First(ping_event) => {
                self.ping.inject_event(peer_id, connection, ping_event);
            }
            EitherOutput::Second(identify_event) => {
                self.identify
                    .inject_event(peer_id, connection, identify_event);
            }
        }
    }

    fn inject_address_change(
        &mut self,
        peer_id: &PeerId,
        connection: &libp2p::core::connection::ConnectionId,
        old: &ConnectedPoint,
        new: &ConnectedPoint,
    ) {
        self.ping
            .inject_address_change(peer_id, connection, old, new);
        self.identify
            .inject_address_change(peer_id, connection, old, new);
    }

    fn inject_dial_failure(
        &mut self,
        peer_id: Option<PeerId>,
        handler: Self::ConnectionHandler,
        error: &libp2p::swarm::DialError,
    ) {
        let (ping_handler, identify_handler) = handler.into_inner();
        self.identify
            .inject_dial_failure(peer_id, identify_handler, error);
        self.ping.inject_dial_failure(peer_id, ping_handler, error);
    }

    fn inject_new_listener(&mut self, id: ListenerId) {
        self.ping.inject_new_listener(id);
        self.identify.inject_new_listener(id);
    }

    fn inject_new_listen_addr(&mut self, id: ListenerId, addr: &Multiaddr) {
        self.ping.inject_new_listen_addr(id, addr);
        self.identify.inject_new_listen_addr(id, addr);
    }

    fn inject_expired_listen_addr(&mut self, id: ListenerId, addr: &Multiaddr) {
        self.ping.inject_expired_listen_addr(id, addr);
        self.identify.inject_expired_listen_addr(id, addr);
    }

    fn inject_new_external_addr(&mut self, addr: &Multiaddr) {
        self.ping.inject_new_external_addr(addr);
        self.identify.inject_new_external_addr(addr);
    }

    fn inject_expired_external_addr(&mut self, addr: &Multiaddr) {
        self.ping.inject_expired_external_addr(addr);
        self.identify.inject_expired_external_addr(addr);
    }

    fn inject_listen_failure(
        &mut self,
        local_addr: &Multiaddr,
        send_back_addr: &Multiaddr,
        handler: Self::ConnectionHandler,
    ) {
        let (ping_handler, identify_handler) = handler.into_inner();
        self.identify
            .inject_listen_failure(local_addr, send_back_addr, identify_handler);
        self.ping
            .inject_listen_failure(local_addr, send_back_addr, ping_handler);
    }

    fn inject_listener_error(&mut self, id: ListenerId, err: &(dyn std::error::Error + 'static)) {
        self.ping.inject_listener_error(id, err);
        self.identify.inject_listener_error(id, err);
    }

    fn inject_listener_closed(&mut self, id: ListenerId, reason: Result<(), &std::io::Error>) {
        self.ping.inject_listener_closed(id, reason);
        self.identify.inject_listener_closed(id, reason);
    }

    fn poll(
        &mut self,
        cx: &mut std::task::Context<'_>,
        params: &mut impl libp2p::swarm::PollParameters,
    ) -> Poll<NetworkBehaviourAction<Self::OutEvent, Self::ConnectionHandler>> {
        if let Poll::Ready(ping_action) = self.ping.poll(cx, params) {
            match ping_action {
                NetworkBehaviourAction::GenerateEvent(PingEvent {
                    peer,
                    result: Ok(PingSuccess::Ping { rtt }),
                }) => {
                    self.insert_latest_ping(&peer, rtt);
                    return Poll::Ready(NetworkBehaviourAction::GenerateEvent(
                        PeerInfoEvent::PeerInfoUpdated { peer_id: peer },
                    ));
                }
                NetworkBehaviourAction::Dial { opts, handler } => {
                    return Poll::Ready(NetworkBehaviourAction::Dial {
                        opts,
                        handler: IntoConnectionHandler::select(
                            handler,
                            self.identify.new_handler(),
                        ),
                    });
                }
                NetworkBehaviourAction::NotifyHandler {
                    peer_id,
                    handler,
                    event,
                } => {
                    return Poll::Ready(NetworkBehaviourAction::NotifyHandler {
                        peer_id,
                        handler,
                        event: EitherOutput::First(event),
                    })
                }
                NetworkBehaviourAction::ReportObservedAddr { address, score } => {
                    return Poll::Ready(NetworkBehaviourAction::ReportObservedAddr {
                        address,
                        score,
                    });
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
                _ => (),
            }
        }

        if let Poll::Ready(identify_action) = self.identify.poll(cx, params) {
            match identify_action {
                NetworkBehaviourAction::GenerateEvent(event) => match event {
                    libp2p::identify::Event::Received {
                        peer_id,
                        info:
                            IdentifyInfo {
                                protocol_version,
                                agent_version,
                                mut listen_addrs,
                                ..
                            },
                    } => {
                        if listen_addrs.len() > MAX_IDENTIFY_ADDRESSES {
                            debug!(
                                "Node {:?} has reported more than {} addresses; it is identified by {:?} and {:?}",
                                peer_id, MAX_IDENTIFY_ADDRESSES, protocol_version, agent_version
                            );
                            listen_addrs.truncate(MAX_IDENTIFY_ADDRESSES);
                        }

                        self.insert_client_version(&peer_id, agent_version);
                        self.insert_peer_addresses(&peer_id, listen_addrs.clone());

                        return Poll::Ready(NetworkBehaviourAction::GenerateEvent(
                            PeerInfoEvent::PeerIdentified {
                                peer_id,
                                addresses: listen_addrs,
                            },
                        ));
                    }
                    libp2p::identify::Event::Error { peer_id, error } => {
                        debug!("Identification with peer {:?} failed => {}", peer_id, error)
                    }
                    _ => {}
                },
                NetworkBehaviourAction::Dial { opts, handler } => {
                    return Poll::Ready(NetworkBehaviourAction::Dial {
                        opts,
                        handler: IntoConnectionHandler::select(self.ping.new_handler(), handler),
                    });
                }
                NetworkBehaviourAction::NotifyHandler {
                    peer_id,
                    handler,
                    event,
                } => {
                    return Poll::Ready(NetworkBehaviourAction::NotifyHandler {
                        peer_id,
                        handler,
                        event: EitherOutput::Second(event),
                    })
                }
                NetworkBehaviourAction::ReportObservedAddr { address, score } => {
                    return Poll::Ready(NetworkBehaviourAction::ReportObservedAddr {
                        address,
                        score,
                    });
                }
                NetworkBehaviourAction::CloseConnection {
                    peer_id,
                    connection,
                } => {
                    debug!("Closing connection to peer: {}", peer_id);
                    return Poll::Ready(NetworkBehaviourAction::CloseConnection {
                        peer_id,
                        connection,
                    });
                }
            }
        }

        Poll::Pending
    }
}
