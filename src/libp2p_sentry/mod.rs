use clap::Parser;
use educe::Educe;
use libp2p::{
    core::{muxing::StreamMuxerBox, transport::Boxed},
    identity::Keypair,
    noise,
    tcp::{GenTcpConfig, TokioTcpTransport},
    yamux::YamuxConfig,
    PeerId, Transport,
};
use std::{
    net::{IpAddr, SocketAddr},
    time::Duration,
};

mod behaviour;
mod discovery;
pub mod orchestrator;
mod peer_info;
mod protocol;
pub mod service;

pub fn build_transport(keypair: &Keypair) -> Boxed<(PeerId, StreamMuxerBox)> {
    let generate_tcp_transport =
        || TokioTcpTransport::new(GenTcpConfig::new().port_reuse(false).nodelay(true));

    let transport = libp2p::dns::TokioDnsConfig::system(
        libp2p::websocket::WsConfig::new(generate_tcp_transport())
            .or_transport(generate_tcp_transport()),
    )
    .unwrap();

    Transport::upgrade(transport, libp2p::core::upgrade::Version::V1)
        .authenticate(generate_noise_config(keypair))
        .multiplex(YamuxConfig::default())
        .timeout(Duration::from_secs(10))
        .boxed()
}

fn generate_noise_config(
    identity_keypair: &Keypair,
) -> noise::NoiseAuthenticated<noise::XX, noise::X25519Spec, ()> {
    let static_dh_keys = noise::Keypair::<noise::X25519Spec>::new()
        .into_authentic(identity_keypair)
        .expect("signing can fail only once during starting a node");
    noise::NoiseConfig::xx(static_dh_keys).into_authenticated()
}

#[cfg(test)]
mod tests {
    // use bytes::BytesMut;
    // use ethereum_interfaces::sentry::InboundMessage;
    // use ethereum_types::H512;
    // use fastrlp::Encodable;
    // use num_traits::FromPrimitive;

    // use crate::sentry::eth::EthMessageId;

    // #[test]
    // fn test_conversion() {
    //     let mut buf = BytesMut::new();
    //     let m = crate::p2p::types::Message::GetBlockHeaders(crate::p2p::types::GetBlockHeaders {
    //         request_id: 0,
    //         params: crate::p2p::types::HeaderRequest::default().into(),
    //     });
    //     m.encode(&mut buf);
    //     let msg_id = ethereum_interfaces::sentry::MessageId::from(m.id()) as i32;

    //     let message = ethereum_interfaces::sentry::OutboundMessageData {
    //         id: msg_id,
    //         data: buf.freeze(),
    //     };
    //     println!("{:?}", message);

    //     let msg = crate::sentry::devp2p::Message::try_from(message).unwrap();

    //     let eth_id = EthMessageId::from_usize(msg.id).unwrap();
    //     let inbound_message = InboundMessage {
    //         id: ethereum_interfaces::sentry::MessageId::from(eth_id) as i32,
    //         data: msg.data,
    //         peer_id: Some(H512::default().into()),
    //     };
    //     println!("inbound message {:?}", inbound_message);

    //     let node_msg = crate::p2p::types::InboundMessage::new(inbound_message, 0).unwrap();
    //     println!("node msg {:?}", node_msg);
    // }
}
