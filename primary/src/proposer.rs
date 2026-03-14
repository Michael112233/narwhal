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
    /// Buffer of parents for the maximum future round (only one entry: the maximum round received so far).
    buffered_parents: Option<(Round, Vec<Digest>)>,
    /// Tracks whether a header has been generated for the current round.
    header_generated_for_current_round: bool,
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
                buffered_parents: None,
                digests: Vec::with_capacity(2 * header_size),
                payload_size: 0,
                solid_step_length,
                store,
                header_generated_for_current_round: false,
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
            debug!("Stored the nodes which can be linked to in the first round");
        } else {
            let parents: Vec<_> = header.parents.iter().cloned().collect();
            let mut merged = HashSet::new();

            for parent in parents {
                let bytes = self.store.notify_read(parent.to_vec()).await.unwrap();
                let cert: Certificate = bincode::deserialize(&bytes).unwrap();
                merged.extend(cert.header.solid_step_vertices);
                debug!("The number of the solid step vertices is {}", cert.header.solid_step_vertices.len());
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
        
        // Mark that we have generated a header for the current round.
        // This flag starts as false, and becomes true after creating a header for the current round.
        self.header_generated_for_current_round = true;
        debug!(
            "Header generated for round {}, header_generated_for_current_round set to true",
            self.round
        );
        
        // After generating a header for the current round, check if we have buffered parents
        // for the next round; if so, load them immediately so we can propose without waiting
        // for Core to resend.
        // IMPORTANT: We only check buffered_parents AFTER the current round's header is generated.
        // This ensures that we don't use buffered_parents to propose the next round's header
        // until the current round's header is complete.
        let next_round = self.round + 1;
        if let Some((buffered_round, buffered_parents)) = self.buffered_parents.take() {
            if buffered_round == next_round {
                debug!(
                    "After generating header for round {}, loaded buffered parents for round {} ({} parents). Now ready to propose round {} header.",
                    self.round,
                    next_round,
                    buffered_parents.len(),
                    next_round
                );
                self.last_parents = buffered_parents;
            } else if buffered_round > next_round {
                // The buffered round is still in the future, keep it
                debug!(
                    "After generating header for round {}, buffered parents for round {} is still in the future (next round is {}), keeping it",
                    self.round,
                    buffered_round,
                    next_round
                );
                self.buffered_parents = Some((buffered_round, buffered_parents));
            } else {
                // The buffered round is in the past, discard it
                debug!(
                    "After generating header for round {}, buffered parents for round {} is in the past (next round is {}), discarding it",
                    self.round,
                    buffered_round,
                    next_round
                );
            }
        } else {
            debug!(
                "After generating header for round {}, no buffered parents for next round {}",
                self.round,
                next_round
            );
        }
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
                // NOTE: This will generate a header for self.round, and only AFTER it's generated
                // will we check buffered_parents to see if we can immediately start proposing the next round.
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
                    if round > self.round {
                        // This is parents info for a future round. Buffer it only if it's the maximum round
                        // we've seen so far (buffered_parents only keeps one entry: the maximum round).
                        let parents_len = parents.len();
                        let should_buffer = match &self.buffered_parents {
                            None => true, // No buffered parents yet, buffer this one
                            Some((buffered_round, _)) => round > *buffered_round, // Only buffer if this round is larger
                        };
                        
                        if should_buffer {
                            if let Some((old_round, _)) = self.buffered_parents.replace((round, parents)) {
                                debug!(
                                    "Replacing buffered parents: old round {}, new round {} ({} parents) while proposer is at round {}",
                                    old_round,
                                    round,
                                    parents_len,
                                    self.round
                                );
                            } else {
                                debug!(
                                    "Buffering parents for future round {} while proposer is at round {} ({} parents)",
                                    round,
                                    self.round,
                                    parents_len
                                );
                            }
                        } else {
                            debug!(
                                "Ignoring parents for round {} while proposer is at round {}: already have buffered parents for a larger round",
                                round,
                                self.round
                            );
                        }
                        continue;
                    }

                    // Advance to the next round.
                    let old_round = self.round;
                    let parents_len = parents.len();
                    self.round = round + 1;
                    // self.round = std::cmp::max(self.round, round) + 1;
                    debug!("Dag moved from round {} to round {}", old_round, self.round);

                    // Only update last_parents if we have already generated a header for the previous round.
                    // This ensures we don't overwrite parents needed for the current round's header.
                    if self.header_generated_for_current_round || old_round == 0 {
                        // Signal that we have enough parent certificates to propose a new header.
                        self.last_parents = parents;
                        // Reset the flag since we're moving to a new round (new round's header hasn't been generated yet).
                        // The flag starts as false, becomes true after creating a header, and resets to false when advancing to a new round.
                        self.header_generated_for_current_round = false;
                        
                        debug!(
                            "Updated last_parents for round {} ({} parents), header_generated_for_current_round reset to false (moved from round {} to round {})",
                            self.round,
                            parents_len,
                            old_round,
                            self.round
                        );
                    } else {
                        // We haven't generated a header for the current round yet (header_generated_for_current_round is false),
                        // so buffer these parents and keep the current last_parents intact.
                        debug!(
                            "Haven't generated header for round {} yet (header_generated_for_current_round is false), buffering parents for round {}",
                            old_round,
                            self.round
                        );
                        // Only buffer if this round is larger than any existing buffered round.
                        let should_buffer = match &self.buffered_parents {
                            None => true, // No buffered parents yet, buffer this one
                            Some((buffered_round, _)) => self.round > *buffered_round, // Only buffer if this round is larger
                        };
                        
                        if should_buffer {
                            if let Some((old_round, _)) = self.buffered_parents.replace((self.round, parents)) {
                                debug!(
                                    "Haven't generated header for round {} yet, replacing buffered parents: old round {}, new round {} ({} parents)",
                                    old_round,
                                    old_round,
                                    self.round,
                                    parents_len
                                );
                            } else {
                                debug!(
                                    "Haven't generated header for round {} yet, buffering parents for round {} ({} parents)",
                                    old_round,
                                    self.round,
                                    parents_len
                                );
                            }
                        } else {
                            debug!(
                                "Haven't generated header for round {} yet, ignoring parents for round {}: already have buffered parents for a larger round",
                                old_round,
                                self.round
                            );
                        }
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
