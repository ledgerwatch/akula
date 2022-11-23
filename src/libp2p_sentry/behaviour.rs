use crate::{p2p::types::PeerFilter, sentry::devp2p::Message};

use super::{
    discovery::{DiscoveryBehaviour, DiscoveryEvent},
    peer_info::{PeerInfo, PeerInfoBehaviour, PeerInfoEvent},
    protocol::ProtocolBehaviour,
};
use hashlink::LruCache;
use libp2p::{
    identity::{Keypair, PublicKey},
    Multiaddr, NetworkBehaviour, PeerId,
};

#[derive(Debug)]
pub enum BehaviourEvent {
    Discovery(DiscoveryEvent),
    PeerInfo(PeerInfoEvent),
    Protocol((PeerId, Message)),
}

macro_rules! impl_from {
    ($event:ty, $variant:ident) => {
        impl From<$event> for BehaviourEvent {
            fn from(src: $event) -> Self {
                Self::$variant(src)
            }
        }
    };
}

impl_from!(DiscoveryEvent, Discovery);
impl_from!(PeerInfoEvent, PeerInfo);
impl_from!((PeerId, Message), Protocol);

#[derive(NetworkBehaviour)]
#[behaviour(out_event = "BehaviourEvent")]
pub struct Behaviour {
    pub protocol: ProtocolBehaviour,
    pub discovery: DiscoveryBehaviour,
    pub peer_info: PeerInfoBehaviour,
}

impl Behaviour {
    pub fn new(local_public_key: PublicKey, bootnodes: Vec<Multiaddr>) -> Self {
        Self {
            protocol: ProtocolBehaviour::new(),
            discovery: DiscoveryBehaviour::new(local_public_key.to_peer_id(), bootnodes),
            peer_info: PeerInfoBehaviour::new(local_public_key),
        }
    }

    pub fn peers(&self) -> &LruCache<PeerId, PeerInfo> {
        self.peer_info.peers()
    }

    pub fn send_by_predicate(&mut self, msg: Message, _: PeerFilter) -> Vec<PeerId> {
        // FIXME: handle predicates.
        let peers = self.peers().iter().map(|(v, _)| *v).collect::<Vec<_>>();
        for peer in &peers {
            self.send_message(peer, msg.clone())
        }
        peers
    }

    pub fn send_message(&mut self, peer: &PeerId, msg: Message) {
        self.protocol.send_message(peer, msg);
    }
}
