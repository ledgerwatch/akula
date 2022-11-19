use futures::FutureExt;
use futures_timer::Delay;
use hashbrown::{HashMap, HashSet};
use libp2p::{
    core::transport::ListenerId,
    kad::{
        handler::KademliaHandlerProto, store::MemoryStore, Kademlia, KademliaConfig, KademliaEvent,
    },
    multiaddr::Protocol,
    swarm::{DialError, NetworkBehaviour, NetworkBehaviourAction},
    Multiaddr, PeerId,
};
use rand::Rng;
use std::{collections::VecDeque, num::NonZeroUsize, task::Poll, time::Duration};
use tracing::*;

const MIN_KAD_INTERVAL: Duration = Duration::from_secs(1);
const MAX_KAD_INTERVAL: Duration = Duration::from_secs(10);

const CONNECTION_TIMEOUT: Duration = Duration::from_secs(30);

const PARALLELISM_FACTOR: NonZeroUsize = NonZeroUsize::new(16).unwrap();

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DiscoveryEvent {
    UnroutablePeer(PeerId),
    Connected(PeerId, Vec<Multiaddr>),
    Disconnected(PeerId),
}

pub struct DiscoveryBehaviour {
    pub kad: Kademlia<MemoryStore>,

    bootnodes: HashMap<PeerId, Multiaddr>,
    connected_peers: HashSet<PeerId>,
    pending_events: VecDeque<DiscoveryEvent>,
    next_kad_random_walk: Delay,
    next_kad_interval: Duration,
    max_peers: usize,
}

impl DiscoveryBehaviour {
    pub fn new(peer_id: PeerId, bootnodes: Vec<Multiaddr>) -> Self {
        let protocol = "/akula/kad/mainnet/kad/1.0.0".as_bytes().to_vec();

        let mut kad_config = KademliaConfig::default();
        kad_config.set_protocol_names(vec![protocol.into()]);
        kad_config.set_parallelism(PARALLELISM_FACTOR);
        kad_config.set_connection_idle_timeout(CONNECTION_TIMEOUT);

        let mut kad = Kademlia::with_config(peer_id, MemoryStore::new(peer_id), kad_config);
        let bootnodes = bootnodes
            .into_iter()
            .filter_map(|addr| PeerId::try_from_multiaddr(&addr).map(|peer_id| (peer_id, addr)))
            .collect::<HashMap<_, _>>();

        for (peer_id, addr) in &bootnodes {
            kad.add_address(peer_id, addr.clone());
        }

        if let Err(e) = kad.bootstrap() {
            warn!("Kademlia bootstrap failed: {}", e);
        }

        Self {
            kad,
            bootnodes,
            connected_peers: Default::default(),
            pending_events: Default::default(),
            next_kad_random_walk: Delay::new(Duration::new(0, 0)),
            next_kad_interval: MIN_KAD_INTERVAL,
            max_peers: 100,
        }
    }

    pub fn add_address(&mut self, peer_id: &PeerId, address: Multiaddr) {
        self.kad.add_address(peer_id, address);
    }
}

impl NetworkBehaviour for DiscoveryBehaviour {
    type ConnectionHandler = KademliaHandlerProto<libp2p::kad::QueryId>;
    type OutEvent = DiscoveryEvent;

    fn new_handler(&mut self) -> Self::ConnectionHandler {
        self.kad.new_handler()
    }

    fn inject_event(
        &mut self,
        peer_id: PeerId,
        connection: libp2p::core::connection::ConnectionId,
        event: <<Self::ConnectionHandler as libp2p::swarm::IntoConnectionHandler>::Handler as libp2p::swarm::ConnectionHandler>::OutEvent,
    ) {
        self.kad.inject_event(peer_id, connection, event);
    }

    fn inject_address_change(
        &mut self,
        peer_id: &PeerId,
        connection_id: &libp2p::core::connection::ConnectionId,
        old: &libp2p::core::ConnectedPoint,
        new: &libp2p::core::ConnectedPoint,
    ) {
        self.kad
            .inject_address_change(peer_id, connection_id, old, new)
    }

    fn addresses_of_peer(&mut self, peer_id: &PeerId) -> Vec<Multiaddr> {
        let mut addrs = HashSet::new();

        if let Some(addr) = self.bootnodes.get(peer_id).cloned() {
            addrs.insert(addr);
        }

        addrs.extend(self.kad.addresses_of_peer(peer_id));
        addrs.retain(|addr| match addr.iter().next() {
            Some(Protocol::Ip4(addr)) if !addr.is_global() => false,
            Some(Protocol::Ip6(addr)) if !addr.is_global() => false,
            _ => true,
        });

        addrs.into_iter().collect()
    }

    fn inject_connection_established(
        &mut self,
        peer_id: &PeerId,
        connection_id: &libp2p::core::connection::ConnectionId,
        endpoint: &libp2p::core::ConnectedPoint,
        failed_addresses: Option<&Vec<Multiaddr>>,
        other_established: usize,
    ) {
        if other_established == 0 {
            self.connected_peers.insert(*peer_id);
            let addresses = self.addresses_of_peer(peer_id);
            self.pending_events
                .push_back(DiscoveryEvent::Connected(*peer_id, addresses));

            trace!("Connected to peer {}", peer_id);
        }

        self.kad.inject_connection_established(
            peer_id,
            connection_id,
            endpoint,
            failed_addresses,
            other_established,
        );
    }

    fn inject_connection_closed(
        &mut self,
        peer_id: &PeerId,
        connection_id: &libp2p::core::connection::ConnectionId,
        connection_point: &libp2p::core::ConnectedPoint,
        handler: <Self::ConnectionHandler as libp2p::swarm::IntoConnectionHandler>::Handler,
        remaining_established: usize,
    ) {
        if remaining_established == 0 {
            self.connected_peers.remove(peer_id);
            self.pending_events
                .push_back(DiscoveryEvent::Disconnected(*peer_id));
            trace!("Disconnected from peer {}", peer_id);
        }

        self.kad.inject_connection_closed(
            peer_id,
            connection_id,
            connection_point,
            handler,
            remaining_established,
        );
    }

    fn inject_new_external_addr(&mut self, addr: &Multiaddr) {
        self.kad.inject_new_external_addr(addr)
    }

    fn inject_expired_listen_addr(&mut self, id: ListenerId, addr: &Multiaddr) {
        self.kad.inject_expired_listen_addr(id, addr);
    }

    fn inject_dial_failure(
        &mut self,
        peer_id: Option<PeerId>,
        handler: Self::ConnectionHandler,
        err: &DialError,
    ) {
        self.kad.inject_dial_failure(peer_id, handler, err)
    }

    fn inject_new_listen_addr(&mut self, id: ListenerId, addr: &Multiaddr) {
        self.kad.inject_new_listen_addr(id, addr)
    }

    fn inject_listener_error(&mut self, id: ListenerId, err: &(dyn std::error::Error + 'static)) {
        self.kad.inject_listener_error(id, err)
    }

    fn inject_listener_closed(&mut self, id: ListenerId, reason: Result<(), &std::io::Error>) {
        self.kad.inject_listener_closed(id, reason)
    }

    fn poll(
        &mut self,
        cx: &mut std::task::Context<'_>,
        params: &mut impl libp2p::swarm::PollParameters,
    ) -> Poll<NetworkBehaviourAction<Self::OutEvent, Self::ConnectionHandler>> {
        if let Some(event) = self.pending_events.pop_front() {
            return Poll::Ready(NetworkBehaviourAction::GenerateEvent(event));
        }

        while self.next_kad_random_walk.poll_unpin(cx).is_ready() {
            if self.connected_peers.len() < self.max_peers {
                let random_peer_id = PeerId::random();
                self.kad.get_closest_peers(random_peer_id);
            }

            self.next_kad_random_walk = Delay::new(self.next_kad_interval);
            self.next_kad_interval =
                rand::thread_rng().gen_range(MIN_KAD_INTERVAL..=MAX_KAD_INTERVAL);
        }

        while let Poll::Ready(kad_action) = self.kad.poll(cx, params) {
            match kad_action {
                NetworkBehaviourAction::GenerateEvent(KademliaEvent::UnroutablePeer { peer }) => {
                    return Poll::Ready(NetworkBehaviourAction::GenerateEvent(
                        DiscoveryEvent::UnroutablePeer(peer),
                    ))
                }
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
                _ => {}
            }
        }

        Poll::Pending
    }
}
