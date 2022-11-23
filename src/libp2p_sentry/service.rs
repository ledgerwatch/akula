use super::{behaviour::Behaviour, build_transport};
use crate::{p2p::types::PeerFilter, sentry::devp2p::Message};
use derive_more::{Deref, DerefMut};
use libp2p::{identity::Keypair, swarm::SwarmBuilder, Multiaddr, PeerId, Swarm};

use tracing::*;

#[derive(Deref, DerefMut)]
pub struct Libp2pService {
    #[deref]
    #[deref_mut]
    pub swarm: Swarm<Behaviour>,

    pub keypair: Keypair,
    pub bootnodes: Vec<Multiaddr>,
}

impl Libp2pService {
    pub fn new(
        keypair: Keypair,
        addr: Multiaddr,
        bootnodes: Vec<Multiaddr>,
    ) -> anyhow::Result<Self> {
        let local_peer_id = PeerId::from(keypair.public());
        let transport = build_transport(&keypair);
        let behaviour = Behaviour::new(keypair.public(), bootnodes.clone());
        let mut swarm = SwarmBuilder::new(transport, behaviour, local_peer_id)
            .executor(Box::new(|f| {
                tokio::spawn(f);
            }))
            .build();
        match swarm.listen_on(addr.clone()) {
            Ok(_) => info!("Listening on {:?}", addr),
            Err(err) => warn!("Failed to listen on {}: {}", addr, err),
        }

        for bootnode in bootnodes.clone() {
            let _ = swarm.dial(bootnode);
        }

        Ok(Self {
            swarm,
            keypair,
            bootnodes,
        })
    }

    #[instrument(level = "trace", skip(self))]
    pub fn send_message(&mut self, msg: Message, pred: PeerFilter) -> Vec<PeerId> {
        self.behaviour_mut().send_by_predicate(msg, pred)
    }
}
