//! Domain model: incidents, resources, responders, assignments and response plans.
//! Field sets follow the RescueMesh brief ("Incident and resource model").

use serde::{Deserialize, Serialize};

pub type NodeId = u32;
pub type ZoneId = u32;
pub type RequestId = String;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Capability {
    FirstAid,
    Electrical,
    Hazmat,
    Water,
    Network,
    Transport,
    General,
}

impl Capability {
    pub const ALL: [Capability; 7] = [
        Capability::FirstAid,
        Capability::Electrical,
        Capability::Hazmat,
        Capability::Water,
        Capability::Network,
        Capability::Transport,
        Capability::General,
    ];
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum ResourceType {
    Medkit,
    Generator,
    Pump,
    Barrier,
    Vehicle,
    NetworkKit,
    HazmatKit,
}

impl ResourceType {
    pub const ALL: [ResourceType; 7] = [
        ResourceType::Medkit,
        ResourceType::Generator,
        ResourceType::Pump,
        ResourceType::Barrier,
        ResourceType::Vehicle,
        ResourceType::NetworkKit,
        ResourceType::HazmatKit,
    ];
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum Category {
    BlockedRoad,
    PowerOutage,
    LabIncident,
    Flooding,
    NetworkOutage,
    MedicalAssist,
    TechnicalAssist,
}

impl Category {
    pub const ALL: [Category; 7] = [
        Category::BlockedRoad,
        Category::PowerOutage,
        Category::LabIncident,
        Category::Flooding,
        Category::NetworkOutage,
        Category::MedicalAssist,
        Category::TechnicalAssist,
    ];

    pub fn required_capabilities(self) -> &'static [Capability] {
        use Capability::*;
        match self {
            Category::BlockedRoad => &[Transport, General],
            Category::PowerOutage => &[Electrical],
            Category::LabIncident => &[Hazmat, FirstAid],
            Category::Flooding => &[Water, Transport],
            Category::NetworkOutage => &[Network, Electrical],
            Category::MedicalAssist => &[FirstAid],
            Category::TechnicalAssist => &[Network, General],
        }
    }

    pub fn required_resources(self) -> &'static [ResourceType] {
        use ResourceType::*;
        match self {
            Category::BlockedRoad => &[Barrier, Vehicle],
            Category::PowerOutage => &[Generator],
            Category::LabIncident => &[HazmatKit, Medkit],
            Category::Flooding => &[Pump, Barrier],
            Category::NetworkOutage => &[NetworkKit, Generator],
            Category::MedicalAssist => &[Medkit, Vehicle],
            Category::TechnicalAssist => &[NetworkKit],
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum IncidentStatus {
    Open,
    Planned,
    Resolved,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Incident {
    pub id: String,
    pub reporter: String,
    pub category: Category,
    pub zone: ZoneId,
    /// 1 (minor) .. 5 (critical)
    pub severity: u8,
    pub created_ms: u64,
    pub status: IncidentStatus,
    pub ttl_ms: u64,
}

impl Incident {
    pub fn is_active(&self, now_ms: u64) -> bool {
        self.status != IncidentStatus::Resolved && now_ms < self.created_ms.saturating_add(self.ttl_ms)
    }

    /// Number of responder slots this incident asks for (1..=3).
    pub fn responders_needed(&self) -> usize {
        1 + (self.severity.clamp(1, 5) as usize) / 2
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Resource {
    pub id: String,
    pub owner: String,
    pub rtype: ResourceType,
    pub capacity: u32,
    pub zone: ZoneId,
    /// Absolute expiry of the offer (ms since epoch).
    pub offer_expires_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Responder {
    pub id: String,
    pub zone: ZoneId,
    pub capabilities: Vec<Capability>,
    /// Responder is committed to an earlier assignment until this time.
    pub busy_until_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum Assignee {
    Responder { id: String },
    Resource { id: String, quantity: u32 },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Assignment {
    pub id: String,
    pub incident_id: String,
    pub assignee: Assignee,
    pub created_ms: u64,
    pub expires_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RankedResponder {
    pub responder_id: String,
    pub zone: ZoneId,
    pub travel_cost: u64,
    pub capability_match: f64,
    /// Lower is better (assignment cost used by the matcher).
    pub cost: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResourceAllocation {
    pub resource_id: String,
    pub rtype: ResourceType,
    pub quantity: u32,
    pub travel_cost: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResponsePlan {
    pub request_id: RequestId,
    pub incident_id: String,
    pub executed_by: NodeId,
    pub priority: f64,
    /// 1-based rank of this incident among the active incidents considered.
    pub rank: usize,
    pub incidents_considered: usize,
    pub responders: Vec<RankedResponder>,
    pub resources: Vec<ResourceAllocation>,
    pub shortfall: Vec<ResourceType>,
    pub route: Vec<ZoneId>,
    pub route_cost: u64,
    /// Fraction of this incident's responder picks that survive weight perturbation.
    pub stability: f64,
    pub actions: Vec<String>,
    pub assignments: Vec<Assignment>,
    pub created_ms: u64,
    pub compute_us: u64,
}
