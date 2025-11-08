use async_graphql::{Enum, SimpleObject};
use chrono::{DateTime, Utc};

#[derive(Enum, Copy, Clone, Eq, PartialEq)]
#[graphql(name = "SubscriptionPlan")]
pub enum SubscriptionPlan {
    Ai,
    Enterprise,
    Free,
    Pro,
    SelfHosted,
    SelfHostedTeam,
    Team,
}

#[derive(Enum, Copy, Clone, Eq, PartialEq)]
#[graphql(name = "SubscriptionRecurring")]
pub enum SubscriptionRecurring {
    Lifetime,
    Monthly,
    Yearly,
}

#[derive(Enum, Copy, Clone, Eq, PartialEq)]
#[graphql(name = "SubscriptionStatus")]
pub enum SubscriptionStatus {
    Active,
    Canceled,
    Incomplete,
    IncompleteExpired,
    PastDue,
    Paused,
    Trialing,
    Unpaid,
}

#[derive(Enum, Copy, Clone, Eq, PartialEq)]
#[graphql(name = "SubscriptionVariant")]
pub enum SubscriptionVariant {
    EA,
    Onetime,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "SubscriptionType")]
pub struct SubscriptionType {
    #[graphql(name = "canceledAt")]
    pub canceled_at: Option<DateTime<Utc>>,
    #[graphql(name = "createdAt")]
    pub created_at: DateTime<Utc>,
    #[graphql(name = "end")]
    pub end: Option<DateTime<Utc>>,
    #[graphql(name = "iapStore")]
    pub iap_store: Option<String>,
    #[graphql(deprecation = "removed")]
    pub id: Option<String>,
    #[graphql(name = "nextBillAt")]
    pub next_bill_at: Option<DateTime<Utc>>,
    pub plan: SubscriptionPlan,
    pub provider: Option<String>,
    pub recurring: SubscriptionRecurring,
    #[graphql(name = "start")]
    pub start: DateTime<Utc>,
    pub status: SubscriptionStatus,
    #[graphql(name = "trialEnd")]
    pub trial_end: Option<DateTime<Utc>>,
    #[graphql(name = "trialStart")]
    pub trial_start: Option<DateTime<Utc>>,
    #[graphql(name = "updatedAt")]
    pub updated_at: DateTime<Utc>,
    pub variant: Option<SubscriptionVariant>,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "SubscriptionPrice")]
pub struct SubscriptionPrice {
    pub amount: Option<i32>,
    pub currency: String,
    #[graphql(name = "lifetimeAmount")]
    pub lifetime_amount: Option<i32>,
    pub plan: SubscriptionPlan,
    #[graphql(name = "type")]
    pub price_type: String,
    #[graphql(name = "yearlyAmount")]
    pub yearly_amount: Option<i32>,
}

impl SubscriptionPrice {
    pub fn new(
        amount: Option<i32>,
        lifetime_amount: Option<i32>,
        yearly_amount: Option<i32>,
        currency: impl Into<String>,
        plan: SubscriptionPlan,
        price_type: impl Into<String>,
    ) -> Self {
        Self {
            amount,
            currency: currency.into(),
            lifetime_amount,
            plan,
            price_type: price_type.into(),
            yearly_amount,
        }
    }
}

pub(crate) fn default_subscription_prices() -> Vec<SubscriptionPrice> {
    vec![
        SubscriptionPrice::new(
            Some(0),
            None,
            None,
            "USD",
            SubscriptionPlan::Free,
            "monthly",
        ),
        SubscriptionPrice::new(
            Some(1200),
            None,
            Some(12000),
            "USD",
            SubscriptionPlan::Pro,
            "monthly",
        ),
        SubscriptionPrice::new(
            Some(3000),
            None,
            Some(30000),
            "USD",
            SubscriptionPlan::Team,
            "monthly",
        ),
    ]
}
