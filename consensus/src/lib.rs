// Copyright(C) Facebook, Inc. and its affiliates.
use config::{Committee, Stake};
use crypto::Hash as _;
use crypto::{Digest, PublicKey};
use log::{debug, info, log_enabled, warn};
use primary::{Certificate, Round};
use std::cmp::max;
use std::collections::{HashMap, HashSet};
use tokio::sync::mpsc::{Receiver, Sender};

#[cfg(test)]
#[path = "tests/consensus_tests.rs"]
pub mod consensus_tests;

/// The representation of the DAG in memory.
type Dag = HashMap<Round, HashMap<PublicKey, (Digest, Certificate)>>;

/// The state that needs to be persisted for crash-recovery.
struct State {
    /// The highest round among all committed certificates. This is used for GC only.
    last_committed_certificate_round: Round,
    /// The round of the last leader whose commit path was accepted.
    last_committed_leader_round: Round,
    // Keeps the last committed round for each authority. This map is used to clean up the dag and
    // ensure we don't commit twice the same certificate.
    last_committed: HashMap<PublicKey, Round>,
    /// Keeps the latest committed certificate (and its parents) for every authority. Anything older
    /// must be regularly cleaned up through the function `update`.
    dag: Dag,
}

impl State {
    fn new(genesis: Vec<Certificate>) -> Self {
        let genesis = genesis
            .into_iter()
            .map(|x| (x.origin(), (x.digest(), x)))
            .collect::<HashMap<_, _>>();

        Self {
            last_committed_certificate_round: 0,
            last_committed_leader_round: 0,
            last_committed: genesis.iter().map(|(x, (_, y))| (*x, y.round())).collect(),
            dag: [(0, genesis)].iter().cloned().collect(),
        }
    }

    /// Update and clean up internal state base on committed certificates.
    fn update(&mut self, certificate: &Certificate, gc_depth: Round) {
        self.last_committed
            .entry(certificate.origin())
            .and_modify(|r| *r = max(*r, certificate.round()))
            .or_insert_with(|| certificate.round());

        let last_committed_certificate_round = *self.last_committed.values().max().unwrap();
        self.last_committed_certificate_round = last_committed_certificate_round;

        for (name, round) in &self.last_committed {
            self.dag.retain(|r, authorities| {
                authorities.retain(|n, _| n != name || r >= round);
                !authorities.is_empty() && r + gc_depth >= last_committed_certificate_round
            });
        }
    }

    fn update_last_committed_leader(&mut self, leader_round: Round) {
        self.last_committed_leader_round = max(self.last_committed_leader_round, leader_round);
    }
}

pub struct Consensus {
    /// The committee information.
    committee: Committee,
    /// The depth of the garbage collector.
    gc_depth: Round,

    /// Receives new certificates from the primary. The primary should send us new certificates only
    /// if it already sent us its whole history.
    rx_primary: Receiver<Certificate>,
    /// Outputs the sequence of ordered certificates to the primary (for cleanup and feedback).
    tx_primary: Sender<Certificate>,
    /// Outputs the sequence of ordered certificates to the application layer.
    tx_output: Sender<Certificate>,

    /// The genesis certificates.
    genesis: Vec<Certificate>,
}

impl Consensus {
    pub fn spawn(
        committee: Committee,
        gc_depth: Round,
        rx_primary: Receiver<Certificate>,
        tx_primary: Sender<Certificate>,
        tx_output: Sender<Certificate>,
    ) {
        tokio::spawn(async move {
            Self {
                committee: committee.clone(),
                gc_depth,
                rx_primary,
                tx_primary,
                tx_output,
                genesis: Certificate::genesis(&committee),
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        // The consensus state (everything else is immutable).
        let mut state = State::new(self.genesis.clone());

        // Listen to incoming certificates.
        while let Some(certificate) = self.rx_primary.recv().await {
            debug!("Processing {:?}", certificate);
            let round = certificate.round();

            // Add the new certificate to the local storage.
            state
                .dag
                .entry(round)
                .or_insert_with(HashMap::new)
                .insert(certificate.origin(), (certificate.digest(), certificate));

            // Emit DAG visualization for extract_final_dag / extract_dag_out (full DAG per round).
            // self.visualize_dag(&state, round);

            // Try to order the dag to commit on every solid-step boundary.
            // - fast path: always try the immediately previous step leader first.
            // - normal path: only on wave boundaries, retry the older wave leader to fill any gap
            //   left by earlier fast-path misses.
            let step_length = self.committee.solid_step_length();
            let wave_length = self.committee.solid_wave_length();
            if round % step_length != 0 {
                continue;
            }
            let mut attempts = Vec::with_capacity(2);
            if round > step_length {
                attempts.push(("fast", round - step_length, round));
            }
            if round % wave_length == 0 && round > wave_length {
                attempts.push(("normal", round - wave_length, round));
            }

            for (path_kind, leader_round, support_round) in attempts {
                if leader_round <= state.last_committed_leader_round {
                    debug!(
                        "Skipping leader_round {} on path={} because last_committed_leader_round={}",
                        leader_round, path_kind, state.last_committed_leader_round
                    );
                    continue;
                }

                let (leader_digest, leader) = match self.leader(leader_round, &state.dag) {
                    Some((digest, cert)) => (digest.clone(), cert.clone()),
                    None => {
                        debug!(
                            "No leader in DAG for leader_round {} (commit path={} requires support_round={})",
                            leader_round, path_kind, support_round
                        );
                        continue;
                    }
                };

                // `leader_digest` is the *certificate digest* returned by `State::dag[leader_round][leader]`.
                // Concretely, `dag` stores `(certificate.digest(), certificate)`.
                //
                // `leader.header.id` is the *header digest* of the leader block itself.
                // In contrast, `solid_step_vertices(_merged)` stored in `Header` are sets of
                // *header ids* (they are generated/merged in the proposer from `header.id`).
                //
                // Validity uses the solid-step vertices stored in certificates from `support_round`,
                // checking whether those vertices include the leader.
                let leader_header_id = leader.header.id.clone();
                if log_enabled!(log::Level::Debug) {
                    // Map authority public keys to the same node-id scheme used by `visualize_dag`.
                    let mut author_to_node: HashMap<PublicKey, usize> = HashMap::new();
                    let mut node_counter = 0usize;
                    for (authority, _) in &self.committee.authorities {
                        author_to_node.insert(*authority, node_counter);
                        node_counter += 1;
                    }

                    let header_pos = self.find_certificate_in_dag(&state, &leader_header_id);
                    let cert_pos = self.find_certificate_in_dag(&state, &leader_digest);

                    debug!(
                        "Commit validity check: path={}, round={}, leader_round={}, support_round={}. \
leader_header_id={:?} -> {:?} (node_id={}); \
leader_digest(cert)= {:?} -> {:?} (node_id={})",
                        path_kind,
                        round,
                        leader_round,
                        support_round,
                        leader_header_id,
                        header_pos.as_ref().map(|(rd, _)| rd),
                        header_pos
                            .map(|(_, a)| author_to_node.get(&a).copied().unwrap_or(999))
                            .unwrap_or(999),
                        leader_digest,
                        cert_pos.as_ref().map(|(rd, _)| rd),
                        cert_pos
                            .map(|(_, a)| author_to_node.get(&a).copied().unwrap_or(999))
                            .unwrap_or(999),
                    );
                }
                let support_round_map = state
                    .dag
                    .get(&support_round)
                    .expect("Support round should exist in the local DAG");
                let mut support_entries = Vec::new();
                let stake: Stake = support_round_map
                    .values()
                    .map(|(_, x)| {
                        let vertices = &x.header.solid_step_vertices;
                        let supports = vertices.contains(&leader_header_id)
                            || vertices.contains(&leader_digest);
                        let node_id = self.author_to_node_id(x.origin());
                        support_entries.push(format!(
                            "[{},{}]:support={} solid=[{}] merged=[{}]",
                            x.round(),
                            node_id,
                            supports,
                            self.render_digest_set(&state, &x.header.solid_step_vertices),
                            self.render_digest_set(&state, &x.header.solid_step_vertices_merged),
                        ));
                        if supports {
                            self.committee.stake(&x.origin())
                        } else {
                            0
                        }
                    })
                    .sum();
                let threshold = self.committee.validity_threshold();
                let leader_node = self.author_to_node_id(leader.origin());
                if stake < threshold {
                    info!(
                        "DAG_COMMIT_CHECK path={} leader_round={} leader_node={} support_round={} support_basis=solid stake={} threshold={} result=insufficient_stake support_set={}",
                        path_kind,
                        leader_round,
                        leader_node,
                        support_round,
                        stake,
                        threshold,
                        support_entries.join(" | ")
                    );
                    if log_enabled!(log::Level::Debug) && stake == 0 {
                        let mut author_to_node: HashMap<PublicKey, usize> = HashMap::new();
                        let mut node_counter = 0usize;
                        for (authority, _) in &self.committee.authorities {
                            author_to_node.insert(*authority, node_counter);
                            node_counter += 1;
                        }

                        debug!(
                            "Validity stake=0 detail: path={}, leader_round={}, support_round={}, leader_header_id={:?}, leader_digest(cert)={:?}",
                            path_kind, leader_round, support_round, leader_header_id, leader_digest
                        );

                        if let Some(round_map) = state.dag.get(&support_round) {
                            let mut certs: Vec<_> = round_map.values().collect();
                            certs.sort_by_key(|(_, cert)| {
                                author_to_node.get(&cert.origin()).copied().unwrap_or(999)
                            });

                            for (cert_digest, cert) in certs {
                                let origin = cert.origin();
                                let node_id = author_to_node.get(&origin).copied().unwrap_or(999);

                                let base = &cert.header.solid_step_vertices;
                                let vertices = base;

                                let contains_leader_header = vertices.contains(&leader_header_id);
                                let contains_leader_digest = vertices.contains(&leader_digest);

                                let mut resolved: Vec<String> = Vec::with_capacity(vertices.len());
                                for d in vertices.iter() {
                                    if let Some((rd, a)) = self.find_certificate_in_dag(&state, d) {
                                        let nid = author_to_node.get(&a).copied().unwrap_or(999);
                                        resolved.push(format!("[{},{}]", rd, nid));
                                    } else {
                                        resolved.push("[?,?]".to_string());
                                    }
                                }
                                resolved.sort();

                                debug!(
                                    "support_round cert: path={} node={} cert_round={} cert_digest={:?} base_len={} contains(leader_header_id)={} contains(leader_digest)={} vertices={}",
                                    path_kind,
                                    node_id,
                                    cert.round(),
                                    cert_digest,
                                    base.len(),
                                    contains_leader_header,
                                    contains_leader_digest,
                                    resolved.join(", ")
                                );
                            }
                        } else {
                            debug!(
                                "Validity stake=0 detail: path={} support_round {} missing from local DAG",
                                path_kind, support_round
                            );
                        }
                    }
                    debug!(
                        "Current stake is {}. Leader {:?} does not have enough support on path={}",
                        stake, leader, path_kind
                    );
                    continue;
                }

                info!(
                    "DAG_COMMIT_CHECK path={} leader_round={} leader_node={} support_round={} support_basis=solid stake={} threshold={} result=committed support_set={}",
                    path_kind,
                    leader_round,
                    leader_node,
                    support_round,
                    stake,
                    threshold,
                    support_entries.join(" | ")
                );

                debug!(
                    "Leader {:?} has enough support on path={}",
                    leader, path_kind
                );
                let leaders_to_commit = if path_kind == "fast" {
                    vec![leader.clone()]
                } else {
                    self.order_leaders(&leader, &state)
                };

                let mut sequence = Vec::new();
                for leader in leaders_to_commit.iter().rev() {
                    for x in self.order_dag(leader, &state) {
                        state.update(&x, self.gc_depth);
                        sequence.push(x);
                    }
                }
                state.update_last_committed_leader(leader_round);

                if log_enabled!(log::Level::Debug) {
                    for (name, round) in &state.last_committed {
                        debug!("Latest commit of {}: Round {}", name, round);
                    }
                }

                for certificate in sequence {
                    let node_id = self.author_to_node_id(certificate.origin());
                    info!(
                        "DAG_COMMITTED round={} node={} digest={:?}",
                        certificate.round(),
                        node_id,
                        certificate.digest()
                    );
                    #[cfg(not(feature = "benchmark"))]
                    info!("Committed {}", certificate.header);

                    #[cfg(feature = "benchmark")]
                    for digest in certificate.header.payload.keys() {
                        info!("Committed {} -> {:?}", certificate.header, digest);
                    }

                    self.tx_primary
                        .send(certificate.clone())
                        .await
                        .expect("Failed to send certificate to primary");

                    if let Err(e) = self.tx_output.send(certificate).await {
                        warn!("Failed to output certificate: {}", e);
                    }
                }
            }
        }
    }

    /// Map authority public key to node id (0..n-1), same as visualize_dag / extract_dag_out.
    fn author_to_node_id(&self, author: PublicKey) -> usize {
        let mut author_to_node: HashMap<PublicKey, usize> = HashMap::new();
        let mut node_counter = 0usize;
        for (authority, _) in &self.committee.authorities {
            author_to_node.insert(*authority, node_counter);
            node_counter += 1;
        }
        *author_to_node.get(&author).unwrap_or(&999)
    }

    /// Returns the certificate (and the certificate's digest) originated by the leader of the
    /// specified round (if any).
    fn leader<'a>(&self, round: Round, dag: &'a Dag) -> Option<&'a (Digest, Certificate)> {
        // TODO: We should elect the leader of round r-2 using the common coin revealed at round r.
        // At this stage, we are guaranteed to have 2f+1 certificates from round r (which is enough to
        // compute the coin). We currently just use round-robin.
        #[cfg(test)]
        let coin = 0;
        #[cfg(not(test))]
        let coin = round;

        // Elect the leader.
        let mut keys: Vec<_> = self.committee.authorities.keys().cloned().collect();
        keys.sort();
        let leader = keys[coin as usize % self.committee.size()];

        // Return its certificate and the certificate's digest.
        dag.get(&round).map(|x| x.get(&leader)).flatten()
    }

    /// For a normal-path commit, order the intermediate step leaders that have not been committed
    /// yet. This lets normal commits fill any gap left by earlier fast-path attempts.
    fn order_leaders(&self, leader: &Certificate, state: &State) -> Vec<Certificate> {
        let mut to_commit = vec![leader.clone()];
        let mut leader = leader;
        let step_length = self.committee.solid_step_length();
        let mut r = leader.round().saturating_sub(step_length);

        while r > state.last_committed_leader_round {
            // Get the certificate proposed by the previous leader.
            let (_, prev_leader) = match self.leader(r, &state.dag) {
                Some(x) => x,
                None => {
                    if r < step_length {
                        break;
                    }
                    r = r.saturating_sub(step_length);
                    continue;
                }
            };

            // Check whether there is a path between the last two leaders.
            if self.linked(leader, prev_leader, &state) {
                to_commit.push(prev_leader.clone());
                leader = prev_leader;
            }

            if r < step_length {
                break;
            }
            r = r.saturating_sub(step_length);
        }
        to_commit
    }

    /// Find a parent certificate by digest in any ancestor round (< child_round).
    fn find_parent_certificate<'a>(
        &self,
        state: &'a State,
        child_round: Round,
        parent_digest: &Digest,
    ) -> Option<&'a (Digest, Certificate)> {
        if child_round <= 1 {
            return None;
        }
        for round in (1..child_round).rev() {
            if let Some(found) = state
                .dag
                .get(&round)
                .and_then(|certs| certs.values().find(|(digest, _)| digest == parent_digest))
            {
                return Some(found);
            }
        }
        None
    }

    /// Checks if there is a path between two leaders.
    /// Unlike the original implementation, this traversal follows weak edges too.
    fn linked(&self, leader: &Certificate, prev_leader: &Certificate, state: &State) -> bool {
        let target = prev_leader.digest();
        let mut stack = vec![leader];
        let mut visited = HashSet::new();

        while let Some(current) = stack.pop() {
            let current_digest = current.digest();
            if !visited.insert(current_digest.clone()) {
                continue;
            }
            if current_digest == target {
                return true;
            }

            for parent in &current.header.parents {
                if let Some((_, parent_cert)) =
                    self.find_parent_certificate(state, current.round(), parent)
                {
                    stack.push(parent_cert);
                }
            }
        }
        false
    }

    /// Flatten the dag referenced by the input certificate. This is a classic depth-first search (pre-order):
    /// https://en.wikipedia.org/wiki/Tree_traversal#Pre-order
    fn order_dag(&self, leader: &Certificate, state: &State) -> Vec<Certificate> {
        debug!("Processing sub-dag of {:?}", leader);
        let mut ordered = Vec::new();
        let mut already_ordered: HashSet<Digest> = HashSet::new();

        let mut buffer = vec![leader];
        while let Some(x) = buffer.pop() {
            debug!("Sequencing {:?}", x);
            ordered.push(x.clone());
            for parent in &x.header.parents {
                let (digest, certificate) =
                    match self.find_parent_certificate(state, x.round(), parent) {
                        Some(x) => x,
                        None => continue, // Parent already GC'ed or not in local DAG.
                    };

                // We skip the certificate if we (1) already processed it or (2) we reached a round that we already
                // committed for this authority.
                let mut skip = already_ordered.contains(digest);
                skip |= state
                    .last_committed
                    .get(&certificate.origin())
                    .map_or_else(|| false, |r| r == &certificate.round());
                if !skip {
                    buffer.push(certificate);
                    already_ordered.insert(digest.clone());
                }
            }
        }

        // Ensure we do not commit garbage collected certificates.
        ordered.retain(|x| x.round() + self.gc_depth >= state.last_committed_certificate_round);

        // Ordering the output by round is not really necessary but it makes the commit sequence prettier.
        ordered.sort_by_key(|x| x.round());
        ordered
    }

    fn visualize_dag(&self, state: &State, current_round: Round) {
        // map from the authority to the node number
        let mut author_to_node: HashMap<PublicKey, usize> = HashMap::new();
        let mut node_counter = 0;
        for (authority, _) in &self.committee.authorities {
            author_to_node.insert(*authority, node_counter);
            node_counter += 1;
        }

        // from current_round to round 1, reverse
        for round in (1..=current_round).rev() {
            if state.dag.contains_key(&round) {
                let round_certs = state.dag.get(&round).unwrap();
                let mut round_output = format!("Round {}:", round);
                let mut vertices = Vec::new();

                let mut sorted_certs: Vec<_> = round_certs.iter().collect();
                sorted_certs.sort_by_key(|(author, _)| *author);

                for (author, (cert_digest, certificate)) in sorted_certs {
                    let node_id = author_to_node.get(author).unwrap_or(&999);
                    let vertex_name = format!("Vertex{}", node_id);

                    // find the parent nodes
                    let mut parents = Vec::new();
                    let mut weak_parents = Vec::new();
                    for parent_digest in &certificate.header.parents {
                        // find the parent certificate in the dag
                        if let Some((parent_round, parent_author)) =
                            self.find_certificate_in_dag(state, parent_digest)
                        {
                            let parent_node_id = author_to_node.get(&parent_author).unwrap_or(&999);
                            let is_weak = parent_round + 1 != round;
                            if is_weak {
                                let weak_entry = format!("[w{},{}]", parent_round, parent_node_id);
                                parents.push(weak_entry.clone());
                                weak_parents.push(weak_entry);
                            } else {
                                parents.push(format!("[{},{}]", parent_round, parent_node_id));
                            }
                        } else {
                            // if the block is genesis, do not need to output
                            if round != 1 {
                                parents.push("[?,?]".to_string());
                            }
                        }
                    }

                    let parent_str = if parents.is_empty() {
                        "[]".to_string()
                    } else {
                        format!("[{}]", parents.join(", "))
                    };

                    // Resolve each solid_step_vertex digest to [round, node_id] for explicit display.
                    let mut solid_vertices = Vec::new();
                    for digest in &certificate.header.solid_step_vertices {
                        if let Some((r, author)) = self.find_certificate_in_dag(state, digest) {
                            let n = author_to_node.get(&author).unwrap_or(&999);
                            solid_vertices.push(format!("[{},{}]", r, n));
                        } else {
                            solid_vertices.push("[?,?]".to_string());
                        }
                    }
                    let solid_str = if solid_vertices.is_empty() {
                        "".to_string()
                    } else {
                        format!(" solid=[{}]", solid_vertices.join(", "))
                    };

                    // Resolve each merged solid_step_vertex digest to [round, node_id].
                    let mut merged_vertices = Vec::new();
                    for digest in &certificate.header.solid_step_vertices_merged {
                        if let Some((r, author)) = self.find_certificate_in_dag(state, digest) {
                            let n = author_to_node.get(&author).unwrap_or(&999);
                            merged_vertices.push(format!("[{},{}]", r, n));
                        } else {
                            merged_vertices.push("[?,?]".to_string());
                        }
                    }
                    let merged_str = if merged_vertices.is_empty() {
                        "".to_string()
                    } else {
                        format!(" merged=[{}]", merged_vertices.join(", "))
                    };

                    let vertex_str = if weak_parents.is_empty() {
                        format!(
                            "({}){} (solid_step_vertices: {}){}{}",
                            vertex_name,
                            parent_str,
                            certificate.header.solid_step_vertices.len(),
                            solid_str,
                            merged_str
                        )
                    } else {
                        format!(
                            "({}){} weak=[{}] (solid_step_vertices: {}){}{}",
                            vertex_name,
                            parent_str,
                            weak_parents.join(", "),
                            certificate.header.solid_step_vertices.len(),
                            solid_str,
                            merged_str
                        )
                    };
                    vertices.push(vertex_str);
                }

                if !vertices.is_empty() {
                    round_output.push_str(&format!(" {} ", vertices.join(" --- ")));
                    info!("{}", round_output);
                }
            }
        }
    }

    fn find_certificate_in_dag(
        &self,
        state: &State,
        digest: &Digest,
    ) -> Option<(Round, PublicKey)> {
        for (round, round_certs) in &state.dag {
            for (author, (cert_digest, certificate)) in round_certs {
                // check if the digest is the header.id or certificate.digest()
                if cert_digest == digest || &certificate.header.id == digest {
                    return Some((*round, *author));
                }
            }
        }
        None
    }

    fn render_digest_set(&self, state: &State, digests: &HashSet<Digest>) -> String {
        let mut resolved = Vec::with_capacity(digests.len());
        for digest in digests {
            if let Some((round, author)) = self.find_certificate_in_dag(state, digest) {
                resolved.push(format!("[{},{}]", round, self.author_to_node_id(author)));
            } else {
                resolved.push("[?,?]".to_string());
            }
        }
        resolved.sort();
        resolved.join(", ")
    }
}
