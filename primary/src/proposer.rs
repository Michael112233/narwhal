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
    /// Short grace period after parents become ready to absorb late certificates.
    parent_grace_delay: Duration,
    /// When the current round first became parent-ready.
    parent_ready_since: Option<Instant>,
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
        let parent_grace_delay_ms = std::env::var("NARWHAL_PROPOSER_PARENT_GRACE_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .or_else(|| {
                std::env::var("NARWHAL_PROPOSER_CRITICAL_DELAY_MS")
                    .ok()
                    .and_then(|v| v.parse::<u64>().ok())
            })
            .unwrap_or(30);

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
                parent_grace_delay: Duration::from_millis(parent_grace_delay_ms),
                parent_ready_since: None,
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
            header.id, origin_node, header.round
        );
        debug!("Created {:?}", header);

        // Maintain solid_step metadata according to the intended semantics:
        // - round 1: solid_step_vertices = parents, merged = parents
        // - init rounds (r % solid_step_len == 0): solid_step_vertices = union(parent.merged), merged = {header}
        // - all other rounds: solid_step_vertices = merged = union(parent.merged)
        debug!("the number of the parents is {}", header.parents.len());
        let parents: Vec<_> = header.parents.iter().cloned().collect();
        let mut merged = HashSet::new();
        let step_index: Round = ((self.round - 1) % self.solid_step_length) + 1;
        let regular_weak_start = self.round.saturating_sub(step_index);

        for parent in &parents {
            // Never block proposer waiting on parent cert materialization here.
            // Missing parents can happen at bootstrap (genesis references) and should not
            // stall header dissemination.
            if let Ok(Some(bytes)) = self.store.read(parent.to_vec()).await {
                if let Ok(cert) = bincode::deserialize::<Certificate>(&bytes) {
                    if self.round > 1 && cert.round() < regular_weak_start {
                        continue;
                    }
                    if cert.header.solid_step_vertices_merged.is_empty() {
                        merged.extend(cert.header.solid_step_vertices.iter().cloned());
                    } else {
                        merged.extend(cert.header.solid_step_vertices_merged.iter().cloned());
                    }
                }
            }
        }

        let is_solid_step_init_round =
            self.round == 1 || (self.round > 1 && self.round % self.solid_step_length == 0);
        if self.round == 1 {
            let parent_set: HashSet<Digest> = parents.into_iter().collect();
            header.store_solid_step_vertex(parent_set.clone());
            header.store_solid_step_merged_vertices(parent_set);
        } else if is_solid_step_init_round {
            header.store_solid_step_vertex(merged);

            let mut self_only: HashSet<Digest> = HashSet::new();
            self_only.insert(header.id.clone());
            header.store_solid_step_merged_vertices(self_only);
        } else {
            header.store_solid_step_vertex(merged.clone());
            header.store_solid_step_merged_vertices(merged);
        }
        debug!(
            "Current round: {}, The number of the solid step vertices is {}",
            self.round,
            header.solid_step_vertices.len()
        );

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
            // In both cases, we wait a short parent grace period first so late certificates can
            // still refresh the parent set before the header is created.
            let enough_parents = !self.last_parents.is_empty();
            let enough_digests = self.payload_size >= self.header_size;
            let timer_expired = timer.is_elapsed();
            let round_open = self.last_proposed_round < self.round;
            let bootstrap_round_ready = self.round == 1 && round_open;
            if round_open && enough_parents && !bootstrap_round_ready {
                if self.parent_ready_since.is_none() {
                    self.parent_ready_since = Some(Instant::now());
                    debug!(
                        "Round {} got enough parents; waiting {:?} grace period before proposal",
                        self.round, self.parent_grace_delay
                    );
                }
            } else {
                self.parent_ready_since = None;
            }
            let parent_grace_elapsed = bootstrap_round_ready
                || (round_open
                    && enough_parents
                    && self
                        .parent_ready_since
                        .map_or(false, |t| t.elapsed() >= self.parent_grace_delay));
            if enough_parents && !write_enough_parent {
                debug!("We have enough parents to propose a new header");
                write_enough_parent = true;
            }
            if enough_digests && !write_enough_digests {
                debug!("We have enough digests to propose a new header");
                write_enough_digests = true;
            }
            if parent_grace_elapsed
                && (bootstrap_round_ready || timer_expired || enough_digests)
                && enough_parents
            {
                write_enough_parent = false;
                write_enough_digests = false;
                if timer_expired {
                    debug!("The timer has expired");
                }

                // Make a new header.
                self.make_header().await;
                self.payload_size = 0;
                self.parent_ready_since = None;

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
