// Copyright(C) Facebook, Inc. and its affiliates.
use crate::messages::{Certificate, Header};
use crate::primary::Round;
use config::{Committee, WorkerId};
use crypto::Hash as _;
use crypto::{Digest, PublicKey, SignatureService};
use log::debug;
#[cfg(feature = "benchmark")]
use log::info;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};

#[cfg(test)]
#[path = "tests/proposer_tests.rs"]
pub mod proposer_tests;

/// The proposer creates new headers and send them to the core for broadcasting and further processing.
pub struct Proposer {
    /// The public key of this primary.
    name: PublicKey,
    /// Service to sign headers.
    signature_service: SignatureService,
    /// The size of the headers' payload.
    header_size: usize,
    /// Optional number of batches required to trigger a header.
    max_header_batches: Option<usize>,
    /// The maximum delay to wait for batches' digests.
    max_header_delay: u64,

    /// Receives the parents to include in the next header (along with their round number).
    rx_core: Receiver<(Vec<Digest>, Round)>,
    /// Receives the batches' digests and serialized payloads from our workers.
    rx_workers: Receiver<(Digest, WorkerId, Vec<u8>)>,
    /// Sends newly created headers to the `Core`.
    tx_core: Sender<Header>,

    /// The current round of the dag.
    round: Round,
    /// Holds the certificates' ids waiting to be included in the next header.
    last_parents: Vec<Digest>,
    /// Holds the batches waiting to be included in the next header.
    digests: Vec<(Digest, WorkerId, Vec<u8>)>,
    /// Keeps track of the size (in bytes) of inlined batches that we received so far.
    payload_size: usize,
}

impl Proposer {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        committee: &Committee,
        signature_service: SignatureService,
        header_size: usize,
        max_header_batches: Option<usize>,
        max_header_delay: u64,
        rx_core: Receiver<(Vec<Digest>, Round)>,
        rx_workers: Receiver<(Digest, WorkerId, Vec<u8>)>,
        tx_core: Sender<Header>,
    ) {
        let genesis = Certificate::genesis(committee)
            .iter()
            .map(|x| x.digest())
            .collect();

        tokio::spawn(async move {
            Self {
                name,
                signature_service,
                header_size,
                max_header_batches,
                max_header_delay,
                rx_core,
                rx_workers,
                tx_core,
                round: 1,
                last_parents: genesis,
                digests: Vec::with_capacity(2 * header_size),
                payload_size: 0,
            }
            .run()
            .await;
        });
    }

    async fn make_header(&mut self) {
        // Make a new header.
        let entries: Vec<_> = self.digests.drain(..).collect();
        let payload = entries
            .iter()
            .map(|(digest, worker_id, _)| (digest.clone(), *worker_id))
            .collect();
        let inline_payload = if entries.is_empty() {
            None
        } else {
            Some(
                entries
                    .into_iter()
                    .map(|(digest, _worker_id, batch)| (digest, batch))
                    .collect(),
            )
        };
        let header = Header::new(
            self.name,
            self.round,
            payload,
            inline_payload,
            self.last_parents.drain(..).collect(),
            &mut self.signature_service,
        )
        .await;
        debug!("Created {:?}. Digest number {}", header, header.payload.len());

        #[cfg(feature = "benchmark")]
        {
            let inline_payload_bytes: usize = header
                .inline_payload
                .as_ref()
                .map(|payload| payload.values().map(|bytes| bytes.len()).sum())
                .unwrap_or(0);
            let serialized_header_bytes = bincode::serialize(&header)
                .map(|bytes| bytes.len())
                .unwrap_or(0);
            info!(
                "VERTEX_STATS round={} payload_entries={} workload_bytes={} serialized_header_bytes={}",
                header.round,
                header.payload.len(),
                inline_payload_bytes,
                serialized_header_bytes
            );
            for digest in header.payload.keys() {
                // NOTE: This log entry is used to compute performance.
                info!("Created {} -> {:?}", header, digest);
            }
        }

        // Send the new header to the `Core` that will broadcast and process it.
        self.tx_core
            .send(header)
            .await
            .expect("Failed to send header");
    }

    // Main loop listening to incoming messages.
    pub async fn run(&mut self) {
        debug!("Dag starting at round {}", self.round);

        let timer = sleep(Duration::from_millis(self.max_header_delay));
        tokio::pin!(timer);

        loop {
            // Check if we can propose a new header. We propose a new header when one of the following
            // conditions is met:
            // 1. We have a quorum of certificates from the previous round and enough batches' digests;
            // 2. We have a quorum of certificates from the previous round and the specified maximum
            // inter-header delay has passed.
            let enough_parents = !self.last_parents.is_empty();
            let enough_digests = match self.max_header_batches {
                Some(max_header_batches) => self.digests.len() >= max_header_batches,
                None => self.payload_size >= self.header_size,
            };
            let timer_expired = timer.is_elapsed();
            if (timer_expired || enough_digests) && enough_parents {
                // Make a new header.
                self.make_header().await;
                self.payload_size = 0;

                // Reschedule the timer.
                let deadline = Instant::now() + Duration::from_millis(self.max_header_delay);
                timer.as_mut().reset(deadline);
            }

            tokio::select! {
                Some((parents, round)) = self.rx_core.recv() => {
                    if round < self.round {
                        continue;
                    }

                    // Advance to the next round.
                    self.round = round + 1;
                    debug!("Dag moved to round {}", self.round);

                    // Signal that we have enough parent certificates to propose a new header.
                    self.last_parents = parents;
                }
                Some((digest, worker_id, batch)) = self.rx_workers.recv() => {
                    if self.digests.is_empty() {
                        debug!("Received first digest for round {}, digest: {:?}", self.round, digest);
                    }
                    self.payload_size += batch.len();
                    self.digests.push((digest, worker_id, batch));
                }
                () = &mut timer => {
                    // Nothing to do.
                }
            }
        }
    }
}
