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
    /// The last round for which this node has already created a header.
    last_proposed_round: Round,
    /// Holds the certificates' ids waiting to be included in the next header.
    last_parents: Vec<Digest>,
    /// Parents received ahead of time, keyed by the next round they unlock.
    pending_parents: HashMap<Round, Vec<Digest>>,
    /// Holds the batches' digests waiting to be included in the next header.
    digests: Vec<(Digest, WorkerId)>,
    /// Keeps track of the size (in bytes) of batches' digests that we received so far.
    payload_size: usize,
    /// The solid step length.
    solid_step_length: u64,
    /// Extra delay for critical rounds to let late certificates arrive.
    critical_round_delay: Duration,
    /// When the current critical round first became parent-ready.
    critical_round_ready_since: Option<Instant>,
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
        let critical_round_delay_ms = std::env::var("NARWHAL_PROPOSER_CRITICAL_DELAY_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(20);

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
                last_proposed_round: 0,
                last_parents: genesis,
                pending_parents: HashMap::new(),
                digests: Vec::with_capacity(2 * header_size),
                payload_size: 0,
                solid_step_length,
                critical_round_delay: Duration::from_millis(critical_round_delay_ms),
                critical_round_ready_since: None,
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

        // Maintain solid_step_vertices:
        // - solid-step initialization rounds reset to the current header ([r,x]),
        // - all other rounds merge from parent certificates.
        debug!("the number of the parents is {}", header.parents.len());
        let is_solid_step_init_round =
            self.round == 1 || (self.round > 1 && self.round % self.solid_step_length == 0);
        if is_solid_step_init_round {
            let mut vertices: HashSet<Digest> = HashSet::new();
            vertices.insert(header.id.clone());
            header.store_solid_step_vertex(vertices);
        } else {
            let parents: Vec<_> = header.parents.iter().cloned().collect();
            let mut merged = HashSet::new();

            for parent in parents {
                // Never block proposer waiting on parent cert materialization here.
                // Missing parents can happen at bootstrap (genesis references) and should not
                // stall header dissemination.
                if let Ok(Some(bytes)) = self.store.read(parent.to_vec()).await {
                    if let Ok(cert) = bincode::deserialize::<Certificate>(&bytes) {
                        let parent_round = cert.round();
                        let parent_id = cert.header.id.clone();
                        merged.extend(cert.header.solid_step_vertices.iter().cloned());
                        // If this parent is a weak edge and it is itself an init-round cert [r,x],
                        // include it directly in solid_step_vertices.
                        let is_weak = parent_round + 1 != self.round;
                        let parent_is_init_round =
                            parent_round == 1 || (parent_round > 1 && parent_round % self.solid_step_length == 0);
                        if is_weak && parent_is_init_round {
                            merged.insert(parent_id);
                        }
                    }
                }
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
        self.last_proposed_round = self.round;
    }

    // Main loop listening to incoming messages.
    pub async fn run(&mut self) {
        debug!("Dag starting at round {}", self.round);

        let timer = sleep(Duration::from_millis(self.max_header_delay));
        tokio::pin!(timer);
        let mut write_enough_parent = false;
        let mut write_enough_digests = false;   

        loop {
            if self.last_proposed_round >= self.round {
                if let Some(parents) = self.pending_parents.remove(&(self.round + 1)) {
                    self.round += 1;
                    self.last_parents = parents;
                    debug!("Dag moved to round {} from buffered parents", self.round);
                }
            }

            // Check if we can propose a new header. We propose a new header when one of the following
            // conditions is met:
            // 1. We have a quorum of certificates from the previous round and enough batches' digests;
            // 2. We have a quorum of certificates from the previous round and the specified maximum
            // inter-header delay has passed.
            let enough_parents = !self.last_parents.is_empty();
            let enough_digests = self.payload_size >= self.header_size;
            let timer_expired = timer.is_elapsed();
            let bootstrap_round_ready = self.round == 1 && self.last_proposed_round < self.round;
            // For the first round of every solid step, wait a short micro-window after
            // parents become ready. This gives late certificates a chance to be included.
            let is_critical_round = self.round > 1
                && self.round % self.solid_step_length == 0
                && self.last_proposed_round < self.round;
            if is_critical_round && enough_parents {
                if self.critical_round_ready_since.is_none() {
                    self.critical_round_ready_since = Some(Instant::now());
                }
            } else {
                self.critical_round_ready_since = None;
            }
            let critical_delay_elapsed = is_critical_round
                && self
                    .critical_round_ready_since
                    .map_or(false, |t| t.elapsed() >= self.critical_round_delay);
            if enough_parents && !write_enough_parent {
                debug!("We have enough parents to propose a new header");
                write_enough_parent = true;
            }
            if enough_digests && !write_enough_digests {
                debug!("We have enough digests to propose a new header");
                write_enough_digests = true;
            }
            if (bootstrap_round_ready || timer_expired || enough_digests || critical_delay_elapsed)
                && enough_parents
            {
                write_enough_parent = false;
                write_enough_digests = false;
                if timer_expired {
                    debug!("The timer has expired");
                }
                if is_critical_round && !timer_expired && !enough_digests {
                    debug!(
                        "Critical round {} delayed by {:?} before proposal",
                        self.round, self.critical_round_delay
                    );
                }
                
                // Make a new header.
                self.make_header().await;
                self.payload_size = 0;
                self.critical_round_ready_since = None;

                // Reschedule the timer.
                let deadline = Instant::now() + Duration::from_millis(self.max_header_delay);
                timer.as_mut().reset(deadline);
            }

            tokio::select! {
                Some((parents, round)) = self.rx_core.recv() => {
                    let next_round = round + 1;
                    if next_round < self.round {
                        debug!("Received stale parents for round {} but we are at round {}", round, self.round);
                        continue;
                    }

                    // If we have not proposed this round yet, keep accepting updated parents
                    // for this exact round. This lets late weak-edge certificates refresh
                    // the parent set before the header is created.
                    if next_round == self.round {
                        if self.last_proposed_round < self.round {
                            let old_len = self.last_parents.len();
                            let mut merged: HashSet<Digest> =
                                self.last_parents.drain(..).collect();
                            merged.extend(parents.into_iter());
                            let merged_len = merged.len();
                            debug!(
                                "Refreshing parents for current round {} before proposal (old={}, merged={})",
                                self.round,
                                old_len,
                                merged_len
                            );
                            self.last_parents = merged.into_iter().collect();
                        } else {
                            debug!(
                                "Received stale parents for current round {} after proposal",
                                self.round
                            );
                        }
                        continue;
                    }

                    // Do not skip rounds: only advance by one round after we already proposed
                    // the current round. Cache out-of-order future parents.
                    if next_round == self.round + 1 && self.last_proposed_round >= self.round {
                        self.round = next_round;
                        debug!("Dag moved to round {}", self.round);
                        let mut merged: HashSet<Digest> = self.last_parents.drain(..).collect();
                        merged.extend(parents.into_iter());
                        self.last_parents = merged.into_iter().collect();
                    } else {
                        debug!(
                            "Buffering parents for future round {} (current round {}, last proposed round {})",
                            next_round,
                            self.round,
                            self.last_proposed_round
                        );
                        match self.pending_parents.get_mut(&next_round) {
                            Some(existing) => {
                                let mut merged: HashSet<Digest> = existing.drain(..).collect();
                                merged.extend(parents.into_iter());
                                *existing = merged.into_iter().collect();
                            }
                            None => {
                                self.pending_parents.insert(next_round, parents);
                            }
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