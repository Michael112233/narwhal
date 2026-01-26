// Copyright(C) Facebook, Inc. and its affiliates.
use crate::error::{DagError, DagResult};
use crate::messages::{Certificate, Header, Vote};
use config::{Committee, Stake};
use crate::primary::Round;
use crypto::Hash as _;
use crypto::{Digest, PublicKey, Signature};
use log::debug;
use std::collections::HashSet;

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
        }
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
            if current_round % committee.solid_step_length() == 1 {
                self.cert_instance.push(certificate.clone());
                debug!("Cert instance size: {}, certificates size: {}", self.cert_instance.len(), self.certificates.len());
            }
            self.weight += committee.stake(&origin);
        } else if certificate.round() >= weak_start && certificate.round() < self.expected_round {
            self.certificates.push(certificate.digest());
            self.weak_certificates.push(certificate.digest());
            if current_round % committee.solid_step_length() == 1 {
                self.weight += committee.stake(&origin);
                self.cert_instance.push(certificate.clone());
                debug!("Cert instance size: {}, certificates size: {}", self.cert_instance.len(), self.certificates.len());
            }
        }
        debug!(
            "Current round: {}, weak range: [{}..={})",
            current_round,
            weak_start,
            current_round - 1
        );

        if current_round % committee.solid_step_length() == 1 && current_round > 1 {
            let mut union_set: HashSet<Digest> = HashSet::new();
            for certificate in &self.cert_instance {
                let cert_first_round_parent: HashSet<Digest> = certificate.header.solid_step_vertices.iter().cloned().collect();
                union_set.extend(cert_first_round_parent);
            }
            // self.has_quorum = (union_set.len() >= committee.processing_threshold(self.expected_round) as usize);
            self.has_quorum = (self.weight >= committee.processing_threshold(self.expected_round));
            debug!("Current round: {}, The number of the solid step vertices is {}", current_round, union_set.len());
        } else {
            self.has_quorum = (self.weight >= committee.processing_threshold(self.expected_round));
            debug!("Current round: {}, The weight is {}", current_round, self.weight);
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
            let mut all = Vec::with_capacity(
                self.certificates.len()
            );
            all.extend(self.certificates.iter().cloned());
            // all.extend(self.weak_certificates.iter().cloned());
            return Ok(Some(all));
        }
        Ok(None)
    }
}
