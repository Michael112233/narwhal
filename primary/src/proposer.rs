// Copyright(C) Facebook, Inc. and its affiliates.
use crate::messages::{Certificate, Header};
use crate::primary::Round;
use config::{Committee, WorkerId};
use crypto::Hash as _;
use crypto::{Digest, PublicKey, SignatureService};
use log::debug;
#[cfg(feature = "benchmark")]
use log::info;
use std::collections::{HashMap, HashSet};
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};

#[cfg(test)]
#[path = "tests/proposer_tests.rs"]
pub mod proposer_tests;

/// The proposer creates new headers and send them to the core for broadcasting and further processing.
pub struct Proposer {
    /// The public key of this primary.
    name: PublicKey,
    /// Node index for logging.
    node_id: Option<usize>,
    /// Service to sign headers.
    signature_service: SignatureService,
    /// The size of the headers' payload.
    header_size: usize,
    /// The maximum delay to wait for batches' digests.
    max_header_delay: u64,

    /// Receives the parents to include in the next header (along with their round number).
    rx_core: Receiver<(Vec<Digest>, Round)>,
    /// Receives the batches' digests from our workers.
    rx_workers: Receiver<(Digest, WorkerId)>,
    /// Sends newly created headers to the `Core`.
    tx_core: Sender<Header>,

    /// The current round of the dag.
    round: Round,
    /// Holds the certificates' ids waiting to be included in the next header.
    last_parents: Vec<Digest>,
    /// Buffer of parents for future rounds (round -> parents digests).
    buffered_parents: HashMap<Round, Vec<Digest>>,
    /// Holds the batches' digests waiting to be included in the next header.
    digests: Vec<(Digest, WorkerId)>,
    /// Keeps track of the size (in bytes) of batches' digests that we received so far.
    payload_size: usize,
    /// The solid step length.
    solid_step_length: u64,
    /// The persistent storage.
    store: Store,
}

impl Proposer {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        committee: &Committee,
        signature_service: SignatureService,
        header_size: usize,
        max_header_delay: u64,
        rx_core: Receiver<(Vec<Digest>, Round)>,
        rx_workers: Receiver<(Digest, WorkerId)>,
        tx_core: Sender<Header>,
        store: Store,
    ) {
        let node_id = committee
            .authorities
            .keys()
            .position(|authority| authority == &name);
        let genesis = Certificate::genesis(committee)
            .iter()
            .map(|x| x.digest())
            .collect();
        let solid_step_length = committee.solid_step_length() as u64;

        tokio::spawn(async move {
            Self {
                name,
                node_id,
                signature_service,
                header_size,
                max_header_delay,
                rx_core,
                rx_workers,
                tx_core,
                round: 1,
                last_parents: genesis,
                buffered_parents: HashMap::new(),
                digests: Vec::with_capacity(2 * header_size),
                payload_size: 0,
                solid_step_length,
                store,
            }
            .run()
            .await;
        });
    }

    async fn make_header(&mut self) {
        // Make a new header.
        let mut header = Header::new(
            self.name,
            self.round,
            self.digests.drain(..).collect(),
            self.last_parents.drain(..).collect(),
            &mut self.signature_service,
        )
        .await;
        let origin_node = self
            .node_id
            .map_or_else(|| "unknown".to_string(), |idx| idx.to_string());
        debug!(
            "Created header {} (origin Node{}, round {})",
            header.id,
            origin_node,
            header.round
        );
        debug!("Created {:?}", header);

        // Store the nodes which can be linked to in the first round
        debug!("the number of the parents is {}", header.parents.len());
        if self.round % self.solid_step_length == 1 {
            let mut vertices: HashSet<Digest> = HashSet::new();
            vertices.insert(header.id.clone());
            header.store_solid_step_vertex(vertices);
        } else {
            let parents: Vec<_> = header.parents.iter().cloned().collect();
            let mut merged = HashSet::new();

            for parent in parents {
                let bytes = self.store.notify_read(parent.to_vec()).await.unwrap();
                let cert: Certificate = bincode::deserialize(&bytes).unwrap();
                merged.extend(cert.header.solid_step_vertices);
            }

            header.store_solid_step_vertex(merged);
        }
        debug!("Current round: {}, The number of the solid step vertices is {}", self.round, header.solid_step_vertices.len());

        #[cfg(feature = "benchmark")]
        for digest in header.payload.keys() {
            // NOTE: This log entry is used to compute performance.
            info!("Created {} -> {:?}", header, digest);
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
            let enough_digests = self.payload_size >= self.header_size;
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
                    if round >= self.round {
                        // This is parents info for a future round. Buffer it and use it
                        // when we eventually advance to that round.
                        debug!(
                            "Buffering parents for future round {} while proposer is at round {} ({} parents)",
                            round,
                            self.round,
                            parents.len()
                        );
                        self.buffered_parents.insert(round, parents);
                        continue;
                    }

                    // Advance to the next round.
                    self.round = round + 1;
                    // self.round = std::cmp::max(self.round, round) + 1;
                    debug!("Dag moved to round {}", self.round);

                    // Signal that we have enough parent certificates to propose a new header.
                    self.last_parents = parents;

                    // After finishing this round, check if we already buffered parents
                    // for the next round; if so, load them immediately so we can
                    // propose without waiting for Core to resend.
                    if let Some(next_parents) = self.buffered_parents.remove(&self.round) {
                        debug!(
                            "Loaded buffered parents for round {} ({} parents)",
                            self.round,
                            next_parents.len()
                        );
                        self.last_parents = next_parents;
                    }
                }
                Some((digest, worker_id)) = self.rx_workers.recv() => {
                    self.payload_size += digest.size();
                    self.digests.push((digest, worker_id));
                }
                () = &mut timer => {
                    // Nothing to do.
                }
            }
        }
    }
}
