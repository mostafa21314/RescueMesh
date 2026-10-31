//! Synthetic data model: incidents, resource offers, responders, assignments,
//! and the response plan the Incident Processing Service produces.
//!
//! Every number that ends up in a [`Plan`] is an integer, so a plan survives a
//! JSON round trip bit-for-bit and replicas can be compared for equality.

use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

pub type NodeId = u32;
pub type Epoch = u64;
pub type ZoneId = u32;
pub type ResponderId = u32;
pub type ResourceId = u32;

/// Identifies one logical client request. Retries reuse it; only the attempt
/// number changes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RequestId {
    pub client_id: Uuid,
    pub seq: u64,
}

impl fmt::Display for RequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // First 8 hex digits are enough to tell clients apart in logs.
        let id = self.client_id.simple().to_string();
        write!(f, "{}/{}", &id[..8], self.seq)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Category {
    Fire,
    Medical,
    Security,
    Flood,
    Structural,
    Hazmat,
}

impl Category {
    pub const ALL: [Category; 6] = [
        Category::Fire,
        Category::Medical,
        Category::Security,
        Category::Flood,
        Category::Structural,
        Category::Hazmat,
    ];

    /// Capabilities the incident's roles need, most important first. Roles
    /// cycle through this list when severity asks for more people.
    pub fn required_capabilities(self) -> &'static [Capability] {
        use Capability::*;
        match self {
            Category::Fire => &[Firefighting, FirstAid],
            Category::Medical => &[FirstAid],
            Category::Security => &[Security],
            Category::Flood => &[WaterRescue, Engineering],
            Category::Structural => &[Engineering, FirstAid],
            Category::Hazmat => &[HazmatHandling, FirstAid],
        }
    }

    /// Resource types the incident needs one unit of each.
    pub fn required_resources(self) -> &'static [ResourceType] {
        use ResourceType::*;
        match self {
            Category::Fire => &[FireExtinguisher, Ambulance],
            Category::Medical => &[Ambulance, Stretcher],
            Category::Security => &[Barrier],
            Category::Flood => &[WaterPump, Generator],
            Category::Structural => &[Generator, Stretcher],
            Category::Hazmat => &[HazmatKit, Barrier],
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Capability {
    FirstAid,
    Firefighting,
    Security,
    WaterRescue,
    Engineering,
    HazmatHandling,
}

impl Capability {
    pub const ALL: [Capability; 6] = [
        Capability::FirstAid,
        Capability::Firefighting,
        Capability::Security,
        Capability::WaterRescue,
        Capability::Engineering,
        Capability::HazmatHandling,
    ];
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ResourceType {
    Ambulance,
    FireExtinguisher,
    Stretcher,
    WaterPump,
    Generator,
    Barrier,
    HazmatKit,
}

impl ResourceType {
    pub const ALL: [ResourceType; 7] = [
        ResourceType::Ambulance,
        ResourceType::FireExtinguisher,
        ResourceType::Stretcher,
        ResourceType::WaterPump,
        ResourceType::Generator,
        ResourceType::Barrier,
        ResourceType::HazmatKit,
    ];
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum IncidentStatus {
    Reported,
    Planned,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Incident {
    pub incident_id: Uuid,
    pub reporter: String,
    pub category: Category,
    pub zone: ZoneId,
    /// 1 (minor) to 5 (critical).
    pub severity: u8,
    /// When the incident was reported, Unix ms.
    pub timestamp_ms: u64,
    pub status: IncidentStatus,
    /// How long the incident stays relevant; assignments for it expire after this.
    pub ttl_ms: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Availability {
    Available,
    Offline,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceOffer {
    pub resource_id: ResourceId,
    pub owner: String,
    #[serde(rename = "type")]
    pub kind: ResourceType,
    pub quantity: u32,
    pub availability: Availability,
    pub zone: ZoneId,
    /// Unix ms after which the offer lapses; `None` = no expiry. The synthetic
    /// campus uses `None`; leases with real expiry belong to Phase 2.
    pub lease_expiry_ms: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Responder {
    pub id: ResponderId,
    pub zone: ZoneId,
    pub capabilities: Vec<Capability>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Assignee {
    Responder { responder_id: ResponderId, role: Capability },
    Resource { resource_id: ResourceId, kind: ResourceType, quantity: u32 },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Assignment {
    pub incident_id: Uuid,
    pub assignee: Assignee,
    pub created_at_ms: u64,
    pub expires_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RankedResponder {
    pub responder_id: ResponderId,
    /// Best cost over the incident's roles (lower is better).
    pub cost: i64,
    /// Expected travel cost to the incident zone, averaged over scenarios.
    pub eta: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Shortfall {
    Responder { role: Capability },
    Resource { kind: ResourceType },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Plan {
    pub incident_id: Uuid,
    /// 0..=1000; higher is more urgent.
    pub priority: u32,
    pub ranked_responders: Vec<RankedResponder>,
    pub assignments: Vec<Assignment>,
    /// Lead responder's path to the incident zone, on the base graph.
    pub route: Vec<ZoneId>,
    pub route_cost: u32,
    pub shortfall: Vec<Shortfall>,
    pub planned_at_ms: u64,
}

impl Plan {
    pub fn responder_ids(&self) -> impl Iterator<Item = ResponderId> + '_ {
        self.assignments.iter().filter_map(|a| match a.assignee {
            Assignee::Responder { responder_id, .. } => Some(responder_id),
            Assignee::Resource { .. } => None,
        })
    }
}
