// Copyright(C) Facebook, Inc. and its affiliates.
use crate::worker::SerializedBatchDigestMessage;
use bytes::Bytes;
use config::Committee;
use network::SimpleSender;
use std::net::SocketAddr;
use tokio::sync::mpsc::Receiver;

// Send batches' digests to the primary.
pub struct PrimaryConnector {
    /// The primary network address.
    primary_address: SocketAddr,
    /// Input channel to receive the digests to send to the primary.
    rx_digest: Receiver<SerializedBatchDigestMessage>,
    /// A network sender to send the baches' digests to the primary.
    network: SimpleSender,
}

impl PrimaryConnector {
    pub fn spawn(
        primary_address: SocketAddr,
        rx_digest: Receiver<SerializedBatchDigestMessage>,
        committee: Committee,
        sender_address: SocketAddr,
    ) {
        tokio::spawn(async move {
            Self {
                primary_address,
                rx_digest,
                network: SimpleSender::new(committee, sender_address),
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        while let Some(digest) = self.rx_digest.recv().await {
            // Send the digest through the network.
            self.network
                .send(self.primary_address, Bytes::from(digest))
                .await;
        }
    }
}
