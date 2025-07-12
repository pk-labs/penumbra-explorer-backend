#![allow(clippy::module_name_repetitions)]
#![allow(clippy::must_use_candidate)]
#![allow(clippy::match_same_arms)]

use crate::api::graphql::scalars::{DateTime, Decimal};
use crate::api::graphql::types::CollectionLimit;
use async_graphql::{Enum, Object, SimpleObject};

#[derive(Debug, Clone, SimpleObject)]
pub struct GovernanceParameters {
    /// The quorum percentage required for a proposal to be valid
    pub valid_quorum: Decimal,
    /// The percentage of votes required for a proposal to pass
    pub passing_threshold: Decimal,
    /// The percentage threshold for slashing
    pub slashing_threshold: Decimal,
    /// The deposit amount required to submit a proposal (in UM)
    pub deposit_amount: Decimal,
    /// The duration of proposal voting in blocks
    pub proposal_duration: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Enum)]
pub enum ProposalKind {
    #[graphql(name = "SIGNALING")]
    Signaling,
    #[graphql(name = "EMERGENCY")]
    Emergency,
    #[graphql(name = "PARAMETER_CHANGE")]
    ParameterChange,
    #[graphql(name = "COMMUNITY_POOL_SPEND")]
    CommunityPoolSpend,
    #[graphql(name = "UPGRADE_PLAN")]
    UpgradePlan,
    #[graphql(name = "FREEZE_IBC_CLIENT")]
    FreezeIbcClient,
    #[graphql(name = "UNFREEZE_IBC_CLIENT")]
    UnfreezeIbcClient,
}

impl ProposalKind {
    pub fn from_database_string(kind: &str) -> Self {
        match kind {
            "Emergency" => Self::Emergency,
            "Parameter Change" => Self::ParameterChange,
            "Community Pool Spend" => Self::CommunityPoolSpend,
            "Upgrade Plan" => Self::UpgradePlan,
            "Freeze IBC Client" => Self::FreezeIbcClient,
            "Unfreeze IBC Client" => Self::UnfreezeIbcClient,
            _ => Self::Signaling,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Enum)]
pub enum ProposalState {
    #[graphql(name = "VOTING")]
    Voting,
    #[graphql(name = "WITHDRAWN")]
    Withdrawn,
    #[graphql(name = "FINISHED")]
    Finished,
    #[graphql(name = "CLAIMED")]
    Claimed,
}

impl ProposalState {
    pub fn from_database_string(state: &str) -> Self {
        match state {
            "Voting" => Self::Voting,
            "Withdrawn" => Self::Withdrawn,
            "Finished" => Self::Finished,
            "Claimed" => Self::Claimed,
            _ => Self::Voting,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Enum)]
pub enum ProposalOutcome {
    #[graphql(name = "PASSED")]
    Passed,
    #[graphql(name = "FAILED")]
    Failed,
    #[graphql(name = "SLASHED")]
    Slashed,
}

impl ProposalOutcome {
    pub fn from_database_string(outcome: Option<&str>) -> Option<Self> {
        match outcome {
            Some("Passed") => Some(Self::Passed),
            Some("Failed") => Some(Self::Failed),
            Some("Slashed") => Some(Self::Slashed),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, SimpleObject)]
pub struct PastProposal {
    pub id: i64,
    pub title: String,
    pub kind: ProposalKind,
    pub state: ProposalState,
    pub outcome: Option<ProposalOutcome>,
    pub total_votes: Decimal,
    pub end_block_height: i64,
    pub end_timestamp: Option<DateTime>,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct ActiveProposal {
    pub id: i64,
    pub title: String,
    pub state: ProposalState,
    pub kind: ProposalKind,
    pub end_block_height: i64,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct PastProposalCollection {
    pub items: Vec<PastProposal>,
    pub total: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Enum)]
pub enum VoteValue {
    #[graphql(name = "YES")]
    Yes,
    #[graphql(name = "NO")]
    No,
    #[graphql(name = "ABSTAIN")]
    Abstain,
}

impl VoteValue {
    pub fn from_database_string(vote: &str) -> Option<Self> {
        match vote {
            "Yes" => Some(Self::Yes),
            "No" => Some(Self::No),
            "Abstain" => Some(Self::Abstain),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, SimpleObject)]
pub struct Vote {
    pub name: String,
    pub id: Option<String>,
    pub effective_voting_power: Decimal,
    pub voting_power_percentage: Decimal,
    pub vote: Option<VoteValue>,
    pub voted_at: DateTime,
    pub tx_hash: Option<String>,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct VoteCollection {
    pub items: Vec<Vote>,
    pub total: i32,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct VoteForTransaction {
    pub id: Option<String>,
    pub vote: Option<VoteValue>,
    pub proposal: i64,
    pub voting_power: Decimal,
}

pub struct ProposalDetail {
    pub id: i64,
    pub title: String,
    pub description: String,
    pub kind: ProposalKind,
    pub deposit_amount: Decimal,
    pub payload: serde_json::Value,
    pub voting_started_block_height: i64,
    pub voting_started_timestamp: DateTime,
    pub voting_ended_block_height: i64,
    pub voting_ended_timestamp: Option<DateTime>,
    pub state: ProposalState,
    pub outcome: Option<ProposalOutcome>,
    pub total_votes: Decimal,
    pub quorum: Decimal,
    pub yes_votes: Decimal,
    pub yes_votes_percentage: Decimal,
    pub abstain_votes: Decimal,
    pub abstain_votes_percentage: Decimal,
    pub no_votes: Decimal,
    pub no_votes_percentage: Decimal,
    pub votes: Vec<Vote>,
}

#[Object]
impl ProposalDetail {
    async fn id(&self) -> i64 {
        self.id
    }

    async fn title(&self) -> &str {
        &self.title
    }

    async fn description(&self) -> &str {
        &self.description
    }

    async fn kind(&self) -> ProposalKind {
        self.kind
    }

    #[graphql(name = "depositAmount")]
    async fn deposit_amount(&self) -> &Decimal {
        &self.deposit_amount
    }

    #[allow(clippy::unused_async)]
    async fn payload(&self) -> async_graphql::Result<serde_json::Value> {
        Ok(self.payload.clone())
    }

    #[graphql(name = "votingStartedBlockHeight")]
    async fn voting_started_block_height(&self) -> i64 {
        self.voting_started_block_height
    }

    #[graphql(name = "votingStartedTimestamp")]
    async fn voting_started_timestamp(&self) -> &DateTime {
        &self.voting_started_timestamp
    }

    #[graphql(name = "votingEndedBlockHeight")]
    async fn voting_ended_block_height(&self) -> i64 {
        self.voting_ended_block_height
    }

    #[graphql(name = "votingEndedTimestamp")]
    async fn voting_ended_timestamp(&self) -> &Option<DateTime> {
        &self.voting_ended_timestamp
    }

    async fn state(&self) -> ProposalState {
        self.state
    }

    async fn outcome(&self) -> &Option<ProposalOutcome> {
        &self.outcome
    }

    #[graphql(name = "totalVotes")]
    async fn total_votes(&self) -> &Decimal {
        &self.total_votes
    }

    async fn quorum(&self) -> &Decimal {
        &self.quorum
    }

    #[graphql(name = "yesVotes")]
    async fn yes_votes(&self) -> &Decimal {
        &self.yes_votes
    }

    #[graphql(name = "yesVotesPercentage")]
    async fn yes_votes_percentage(&self) -> &Decimal {
        &self.yes_votes_percentage
    }

    #[graphql(name = "abstainVotes")]
    async fn abstain_votes(&self) -> &Decimal {
        &self.abstain_votes
    }

    #[graphql(name = "abstainVotesPercentage")]
    async fn abstain_votes_percentage(&self) -> &Decimal {
        &self.abstain_votes_percentage
    }

    #[graphql(name = "noVotes")]
    async fn no_votes(&self) -> &Decimal {
        &self.no_votes
    }

    #[graphql(name = "noVotesPercentage")]
    async fn no_votes_percentage(&self) -> &Decimal {
        &self.no_votes_percentage
    }

    #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    async fn votes(&self, limit: Option<CollectionLimit>) -> VoteCollection {
        let limit = limit.unwrap_or(CollectionLimit {
            length: Some(10),
            offset: Some(0),
        });
        
        let length = limit.length.unwrap_or(10) as usize;
        let offset = limit.offset.unwrap_or(0) as usize;
        
        let total = self.votes.len() as i32;
        let items = if offset < self.votes.len() {
            let end = std::cmp::min(offset + length, self.votes.len());
            self.votes[offset..end].to_vec()
        } else {
            Vec::new()
        };
        
        VoteCollection { items, total }
    }
}
