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
    /// The last committed round.
    last_committed_round: Round,
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
            last_committed_round: 0,
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

        let last_committed_round = *self.last_committed.values().max().unwrap();
        self.last_committed_round = last_committed_round;

        // for (name, round) in &self.last_committed {
        //     self.dag.retain(|r, authorities| {
        //         authorities.retain(|n, _| n != name || r >= round);
        //         !authorities.is_empty() && r + gc_depth >= last_committed_round
        //     });
        // }
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

            self.visualize_dag(&state, round);

            // Try to order the dag to commit. Start from the highest round for which we have at least
            // 2f+1 certificates. This is because we need them to reveal the common coin.
            let r = round - self.committee.solid_step_length();

            // We only elect leaders for rounds such that round % solid_wave_length == 1.
            if r % self.committee.solid_wave_length() != 0
                || r < 2 * self.committee.solid_wave_length()
            {
                continue;
            }

            // debug!("Current round: {}, It is time to elect the leader", r);

            // Get the certificate's digest of the leader of round r-2. If we already ordered this leader,
            // there is nothing to do.
            let leader_round = r - self.committee.solid_wave_length();
            if leader_round <= state.last_committed_round {
                continue;
            }
            let (leader_digest, leader) = match self.leader(leader_round, &state.dag) {
                Some(x) => x,
                None => {
                    debug!("No leader in DAG for leader_round {} (commit requires leader for r={})", leader_round, r);
                    continue;
                }
            };

            // Check if the leader has f+1 support from the solid-step projection round:
            // support_round = r - solid_wave_length + solid_step_length.
            // A certificate supports the leader if its solid_step_vertices contains leader_digest.
            let support_round =
                r - self.committee.solid_wave_length() + self.committee.solid_step_length();
            let stake: Stake = state
                .dag
                .get(&support_round)
                .expect("We should have the whole history by now")
                .values()
                .filter(|(_, x)| {
                    if !x.header.solid_step_vertices_merged.is_empty() {
                        info!("Using solid_step_vertices_merged");
                        info!("solid_step_vertices_merged: {:?}, leader_digest: {:?}", x.header.solid_step_vertices_merged, leader_digest);
                        x.header.solid_step_vertices_merged.contains(&leader_digest)
                    } else {
                        // Backward-compatible fallback for legacy headers.
                        debug!("error: solid_step_vertices_merged is empty");
                        x.header.solid_step_vertices.contains(&leader_digest)
                    }
                })
                .map(|(_, x)| self.committee.stake(&x.origin()))
                .sum();
            // If it is the case, we can commit the leader. But first, we need to recursively go back to
            // the last committed leader, and commit all preceding leaders in the right order. Committing
            // a leader block means committing all its dependencies.
            if stake < self.committee.validity_threshold() {
                debug!("Current stake is {}. Leader {:?} does not have enough support", stake, leader);
                continue;
            }
            

            // Get an ordered list of past leaders that are linked to the current leader.
            debug!("Leader {:?} has enough support", leader);
            let mut sequence = Vec::new();
            for leader in self.order_leaders(leader, &state).iter().rev() {
                // Starting from the oldest leader, flatten the sub-dag referenced by the leader.
                for x in self.order_dag(leader, &state) {
                    // Update and clean up internal state.
                    state.update(&x, self.gc_depth);

                    // Add the certificate to the sequence.
                    sequence.push(x);
                }
            }

            // Log the latest committed round of every authority (for debug).
            if log_enabled!(log::Level::Debug) {
                for (name, round) in &state.last_committed {
                    debug!("Latest commit of {}: Round {}", name, round);
                }
            }

            // Output the sequence in the right order.
            for certificate in sequence {
                #[cfg(not(feature = "benchmark"))]
                info!("Committed {}", certificate.header);

                #[cfg(feature = "benchmark")]
                for digest in certificate.header.payload.keys() {
                    // NOTE: This log entry is used to compute performance.
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

    /// Order the past leaders that we didn't already commit.
    fn order_leaders(&self, leader: &Certificate, state: &State) -> Vec<Certificate> {
        let mut to_commit = vec![leader.clone()];
        let mut leader = leader;
        for r in (state.last_committed_round + self.committee.solid_wave_length()..leader.round())
            .rev()
            .step_by(self.committee.solid_wave_length() as usize)
        {
            // Get the certificate proposed by the previous leader.
            let (_, prev_leader) = match self.leader(r, &state.dag) {
                Some(x) => x,
                None => continue,
            };

            // Check whether there is a path between the last two leaders.
            if self.linked(leader, prev_leader, &state) {
                to_commit.push(prev_leader.clone());
                leader = prev_leader;
            }
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
                let (digest, certificate) = match self.find_parent_certificate(state, x.round(), parent)
                {
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
        ordered.retain(|x| x.round() + self.gc_depth >= state.last_committed_round);

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
                        if let Some((parent_round, parent_author)) = self.find_certificate_in_dag(state, parent_digest) {
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
                    
                    let vertex_str = if weak_parents.is_empty() {
                        format!(
                            "({}){} (solid_step_vertices: {}){}",
                            vertex_name,
                            parent_str,
                            certificate.header.solid_step_vertices.len(),
                            solid_str
                        )
                    } else {
                        format!(
                            "({}){} weak=[{}] (solid_step_vertices: {}){}",
                            vertex_name,
                            parent_str,
                            weak_parents.join(", "),
                            certificate.header.solid_step_vertices.len(),
                            solid_str
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

    fn find_certificate_in_dag(&self, state: &State, digest: &Digest) -> Option<(Round, PublicKey)> {
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
}
