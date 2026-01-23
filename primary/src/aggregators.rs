// Copyright(C) Facebook, Inc. and its affiliates.
use crate::error::{DagError, DagResult};
use crate::messages::{Certificate, Header, Vote};
use config::{Committee, Stake};
use crate::primary::Round;
use crypto::Hash as _;
use crypto::{Digest, PublicKey, Signature};
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

        if certificate.round() == self.expected_round {
            self.certificates.push(certificate.digest());
            self.weight += committee.stake(&origin);
        } else if certificate.round() < self.expected_round {
            // self.weak_certificates.push(certificate.digest());
            if certificate.round() + committee.solid_step_length() == self.expected_round {
                self.certificates.push(certificate.digest());
                self.weight += committee.stake(&origin);
            }
        }

        if self.weight >= committee.processing_threshold() {
            self.has_quorum = true;
        }

        if self.has_quorum {
            let mut all = Vec::with_capacity(
                self.certificates.len() + self.weak_certificates.len(),
            );
            all.extend(self.certificates.iter().cloned());
            all.extend(self.weak_certificates.iter().cloned());
            return Ok(Some(all));
        }
        Ok(None)
    }
}
