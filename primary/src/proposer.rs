// Copyright(C) Facebook, Inc. and its affiliates.
use crate::messages::{Certificate, Header};
use crate::primary::Round;
use config::{Committee, WorkerId};
use crypto::Hash as _;
use crypto::{Digest, PublicKey, SignatureService};
use log::debug;
#[cfg(feature = "benchmark")]
use log::info;
use std::collections::HashSet;
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

        debug!("Start proposer! at round 1");
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
        // Get the first parent's round for logging
        let first_parent_round = if let Some(first_parent) = self.last_parents.first() {
            if let Ok(Some(bytes)) = self.store.read(first_parent.to_vec()).await {
                if let Ok(cert) = bincode::deserialize::<Certificate>(&bytes) {
                    Some(cert.round())
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };
        
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
        
        let parents_info = if let Some(round) = first_parent_round {
            format!("first parent round: {}", round)
        } else {
            "first parent round: ?".to_string()
        };
        
        debug!(
            "Created header {} (origin Node{}, round {}), {}",
            header.id,
            origin_node,
            header.round,
            parents_info
        );
        debug!("Created {:?}", header);

        // Store the nodes which can be linked to in the first round
        debug!("the number of the parents is {}", header.parents.len());
        if self.round % self.solid_step_length == 1 {
            let mut vertices: HashSet<Digest> = HashSet::new();
            vertices.insert(header.id.clone());
            header.store_solid_step_vertex(vertices);
            debug!("Stored the nodes which can be linked to in the first round");
        } else {
            let parents: Vec<_> = header.parents.iter().cloned().collect();
            let mut merged = HashSet::new();

            for parent in parents {
                let bytes = self.store.notify_read(parent.to_vec()).await.unwrap();
                let cert: Certificate = bincode::deserialize(&bytes).unwrap();
                let solid_step_vertices_count = cert.header.solid_step_vertices.len();
                merged.extend(cert.header.solid_step_vertices);
                debug!("The number of the solid step vertices is {}", solid_step_vertices_count);
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
                // Make a new header for the current round.
                debug!(
                    "Conditions met to propose header for round {}: enough_parents={}, enough_digests={}, timer_expired={}",
                    self.round,
                    enough_parents,
                    enough_digests,
                    timer_expired
                );
                self.make_header().await;
                self.payload_size = 0;

                // Reschedule the timer.
                let deadline = Instant::now() + Duration::from_millis(self.max_header_delay);
                timer.as_mut().reset(deadline);
            } 
            // else {
            //     // Log why we cannot propose a header
            //     let mut reasons = Vec::new();
            //     if !enough_parents {
            //         reasons.push(format!("not enough parents (last_parents.len()={})", self.last_parents.len()));
            //     }
            //     if !enough_digests {
            //         reasons.push(format!("not enough digests (payload_size={}, header_size={})", self.payload_size, self.header_size));
            //     }
            //     if !timer_expired {
            //         reasons.push("timer not expired".to_string());
            //     }
            //     debug!(
            //         "Cannot propose header for round {}: {}",
            //         self.round,
            //         reasons.join(", ")
            //     );
            // }

            tokio::select! {
                Some((parents, round)) = self.rx_core.recv() => {
                    debug!("Received parents for round {} while proposer is at round {}", round, self.round);
                    
                    // Advance to the next round.
                    let old_round = self.round;
                    let parents_len = parents.len();
                    self.round = round + 1;
                    debug!("Dag moved from round {} to round {}", old_round, self.round);

                    // Update last_parents with the new parents.
                    self.last_parents = parents;
                    debug!(
                        "Updated last_parents for round {} ({} parents)",
                        self.round,
                        parents_len
                    );
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
