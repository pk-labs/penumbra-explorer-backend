use async_graphql::{Enum, SimpleObject};
use crate::api::graphql::scalars::{DateTime, Decimal};

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
            _ => Self::Signaling, // Default case for "Signaling" and unknown types
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
            _ => Self::Voting, // Default case
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
            _ => None, // NULL in database
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