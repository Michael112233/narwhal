// Copyright(C) Facebook, Inc. and its affiliates.
use crate::messages::Certificate;
use crate::primary::PrimaryWorkerMessage;
use bytes::Bytes;
use config::Committee;
use crypto::PublicKey;
use network::SimpleSender;
use log::info;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;
use tokio::sync::watch;

/// Receives the highest round reached by consensus and update it for all tasks.
pub struct GarbageCollector {
    /// The current consensus round (used for cleanup).
    consensus_round: Arc<AtomicU64>,
    /// Receives the ordered certificates from consensus.
    rx_consensus: Receiver<Certificate>,
    /// The network addresses of our workers.
    addresses: Vec<SocketAddr>,
    /// A network sender to notify our workers of cleanup events.
    network: SimpleSender,
    /// Wave notifier for bandwidth monitoring.
    wave_notifier: Option<watch::Sender<u64>>,
}

impl GarbageCollector {
    pub fn spawn(
        name: &PublicKey,
        committee: &Committee,
        consensus_round: Arc<AtomicU64>,
        rx_consensus: Receiver<Certificate>,
    ) {
        Self::spawn_with_wave_notifier(name, committee, consensus_round, rx_consensus, None);
    }

    pub fn spawn_with_wave_notifier(
        name: &PublicKey,
        committee: &Committee,
        consensus_round: Arc<AtomicU64>,
        rx_consensus: Receiver<Certificate>,
        wave_notifier: Option<watch::Sender<u64>>,
    ) {
        let addresses = committee
            .our_workers(name)
            .expect("Our public key or worker id is not in the committee")
            .iter()
            .map(|x| x.primary_to_worker)
            .collect();
        let sender_address = committee
            .primary(name)
            .expect("Our public key is not in the committee")
            .primary_to_primary;
        let committee_clone = committee.clone();

        tokio::spawn(async move {
            Self {
                consensus_round,
                rx_consensus,
                addresses,
                network: SimpleSender::new(committee_clone, sender_address),
                wave_notifier,
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        let mut last_committed_round = 0;
        while let Some(certificate) = self.rx_consensus.recv().await {
            // TODO [issue #9]: Re-include batch digests that have not been sequenced into our next block.

            let round = certificate.round();
            if round > last_committed_round {
                last_committed_round = round;
                let current_wave = round; // Use round as wave ID

                // Log wave information for bandwidth utilization analysis
                info!("WAVE_UPDATE: wave={} round={}", current_wave, round);

                // Notify bandwidth monitor about wave update
                if let Some(ref sender) = self.wave_notifier {
                    let _ = sender.send(current_wave);
                }

                // Trigger cleanup on the primary.
                self.consensus_round.store(round, Ordering::Relaxed);

                // Trigger cleanup on the workers..
                let bytes = bincode::serialize(&PrimaryWorkerMessage::Cleanup(round))
                    .expect("Failed to serialize our own message");
                self.network
                    .broadcast(self.addresses.clone(), Bytes::from(bytes))
                    .await;
            }
        }
    }
}
