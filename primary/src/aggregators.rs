// Copyright(C) Facebook, Inc. and its affiliates.
use crate::error::{DagError, DagResult};
use crate::messages::{Certificate, Header, Vote};
use config::{Committee, Stake};
use crate::primary::Round;
use crypto::Hash as _;
use crypto::{Digest, PublicKey, Signature};
use log::debug;
use std::collections::HashSet;
use std::time::{Duration, Instant};

/// Aggregates votes for a particular header into a certificate.
pub struct VotesAggregator {
    weight: Stake,
    votes: Vec<(PublicKey, Signature)>,
    used: HashSet<PublicKey>,
}

impl VotesAggregator {
    pub fn new() -> Self {
        Self {
            weight: 0,
            votes: Vec::new(),
            used: HashSet::new(),
        }
    }

    pub fn append(
        &mut self,
        vote: Vote,
        committee: &Committee,
        header: &Header,
    ) -> DagResult<Option<Certificate>> {
        let author = vote.author;

        // Ensure it is the first time this authority votes.
        ensure!(self.used.insert(author), DagError::AuthorityReuse(author));

        self.votes.push((author, vote.signature));
        self.weight += committee.stake(&author);
        if self.weight >= committee.quorum_threshold() {
            self.weight = 0; // Ensures quorum is only reached once.
            return Ok(Some(Certificate {
                header: header.clone(),
                votes: self.votes.clone(),
            }));
        }
        Ok(None)
    }
}

/// Aggregate certificates and check if we reach a quorum.
pub struct CertificatesAggregator {
    expected_round: Round,
    weight: Stake,
    certificates: Vec<Digest>,
    weak_certificates: Vec<Digest>,
    cert_instance: Vec<Certificate>,
    used: HashSet<PublicKey>,
    has_quorum: bool,
    /// Wait for several seconds after meeting the condition
    quorum_reached_time: Option<Instant>,
    wait_duration: Duration,
    /// Last computed union of solid_step_vertices (for debug / final_dag display).
    last_union_set: Option<Vec<Digest>>,
}

impl CertificatesAggregator {
    pub fn new(expected_round: Round) -> Self {
        Self {
            expected_round,
            weight: 0,
            certificates: Vec::new(),
            weak_certificates: Vec::new(),
            cert_instance: Vec::new(),
            used: HashSet::new(),
            has_quorum: false,
            quorum_reached_time: None,
            wait_duration: Duration::from_millis(20),
            last_union_set: None,
        }
    }

    /// Returns the last computed union of solid_step_vertices (when advancing to a solid round).
    /// Used by core to resolve digests to [round, node_id] for debug and final_dag.
    pub fn last_solid_step_union_digests(&self) -> Option<&[Digest]> {
        self.last_union_set.as_deref()
    }

    pub fn append(
        &mut self,
        certificate: Certificate,
        committee: &Committee,
    ) -> DagResult<Option<Vec<Digest>>> {
        let origin = certificate.origin();

        // Ensure it is the first time this authority votes.
        if !self.used.insert(origin) {
            return Ok(None);
        }
        let current_round = self.expected_round + 1;
        let step_id = (current_round - 1) % committee.solid_step_length();
        let weak_start: Round;
        if step_id == 0 {
            weak_start = current_round - committee.solid_step_length();
        } else {
            weak_start = current_round - step_id;
        }

        if certificate.round() == self.expected_round {
            self.certificates.push(certificate.digest());
            if current_round % committee.solid_step_length() == 0 {
                self.cert_instance.push(certificate.clone());
                // debug!("Cert instance size: {}, certificates size: {}", self.cert_instance.len(), self.certificates.len());
            }
            self.weight += committee.stake(&origin);
        } else if certificate.round() >= weak_start && certificate.round() < self.expected_round {
            self.certificates.push(certificate.digest());
            self.weak_certificates.push(certificate.digest());
            if current_round % committee.solid_step_length() == 0 {
                self.weight += committee.stake(&origin);
                self.cert_instance.push(certificate.clone());
                // debug!("Cert instance size: {}, certificates size: {}", self.cert_instance.len(), self.certificates.len());
            }
        }
        debug!(
            "Current round: {}, weak range: [{}..={})",
            current_round,
            weak_start,
            current_round - 1
        );

        let threshold = committee.processing_threshold(current_round);
        let is_solid_step = current_round % committee.solid_step_length() == 0 && current_round > 1;
        debug!(
            "Advance to round {}: require weight >= {}, solid_step={})",
            current_round, threshold, is_solid_step
        );
        if is_solid_step {
            let mut union_set: HashSet<Digest> = HashSet::new();
            for certificate in &self.cert_instance {
                let cert_first_round_parent: HashSet<Digest> = certificate.header.solid_step_vertices.iter().cloned().collect();
                union_set.extend(cert_first_round_parent);
            }
            self.last_union_set = Some(union_set.iter().cloned().collect());
            // self.has_quorum = (self.weight >= min_weight);
            self.has_quorum = (union_set.len() >= committee.processing_threshold(current_round) as usize);
            debug!("Current round: {}, The number of the solid step vertices is {}", current_round, union_set.len());
        } else {
            self.last_union_set = None;
            // self.has_quorum = (self.weight >= min_weight);
            self.has_quorum = (self.weight >= committee.processing_threshold(current_round));
            debug!("Current round: {}, The weight is {}, self_has_quorum: {}", current_round, self.weight, self.has_quorum);
        }
        // Modify processing condition
        // if self.expected_round % committee.solid_step_length() as u64 == 1 && self.expected_round > 1 {
        //     if self.certificates..solid_step_vertices.len() >= committee.processing_threshold(self.expected_round as u64) {
        //         self.has_quorum = true;
        //     }
        // } else {
        //     if self.weight >= committee.processing_threshold(self.expected_round as u64) {
        //         self.has_quorum = true;
        //     }
        // }

        if self.has_quorum {
            if self.quorum_reached_time.is_none() {
                self.quorum_reached_time = Some(Instant::now());
            }
            let mut all = Vec::with_capacity(
                self.certificates.len()
            );
            all.extend(self.certificates.iter().cloned());
            // if self.quorum_reached_time.unwrap().elapsed() >= self.wait_duration || self.weight >= committee.max_threshold() {
            return Ok(Some(all));
            // }
        }
        Ok(None)
    }
}
