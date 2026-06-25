# Kafka vs Eventarc — Integration Pattern Comparison

**API Integration Governance | PI Technology | Definity | June 2026**  
**Author:** Bashkar Sampath, Manager Technology — TS Personal Insurance Technology

---

## Context

Definity's enterprise integration platform uses Kafka as the standard for event-driven integrations. As teams adopt GCP as the long-term cloud platform, Eventarc — Google Cloud's native event routing service — presents a complementary alternative for specific workload profiles.

This document proposes Eventarc starting with GCS-triggered file ingestion pipelines as the initial approved use case. This is deliberately scoped as a starting capability. Eventarc supports 90+ GCP event sources and multiple destination types — broader adoption can follow once the pattern is established and proven at Definity.

---

## Problem

How should teams choose between Kafka and Eventarc for event-driven integrations?

- When does Kafka's enterprise streaming capability justify its cost and operational overhead?
- When does Eventarc's serverless, GCP-native routing provide a better fit?
- What criteria should govern the choice to ensure consistency and supportability?

---

## Kafka — Enterprise Event Streaming Platform

Kafka is Definity's standard enterprise event streaming platform, managed by the API Integration team. Recommended for high-throughput, real-time, and multi-cloud event-driven integrations.

### Pros

| | |
|---|---|
| ✓ | Enterprise standard — consistent pattern across all domains and cloud environments |
| ✓ | High throughput — designed for real-time, high-volume event streams |
| ✓ | Multi-cloud portability — works across AWS, GCP, and Azure without re-platforming |
| ✓ | Multiple consumers — fan-out to many downstream systems from a single topic |
| ✓ | Message replay — events retained and replayable within the configured retention window |
| ✓ | Mature governance — established patterns, runbooks, and organizational expertise |
| ✓ | Cross-system backbone — ideal for integrations spanning multiple domains and teams |

### Cons

| | |
|---|---|
| ✗ | Fixed infrastructure cost — broker, topic, and connector costs incurred regardless of message frequency |
| ✗ | Onboarding overhead — intake process, connector provisioning, and configuration per integration |
| ✗ | Operational complexity — connector lifecycle, offset tracking, DLQ handling, version upgrades |
| ✗ | Disproportionate for low frequency — cost-to-value ratio poor for weekly or quarterly triggers |
| ✗ | Not GCP-native — requires a connector layer between GCP services and Kafka, adding hops |
| ✗ | Lead time — intake and provisioning process before integration work can begin |

### Best Fit Workloads

- Real-time or near-real-time event processing
- High-throughput pipelines with frequent events
- Multi-consumer fan-out to many downstream systems
- Cross-cloud or cross-system enterprise integrations
- Workloads requiring long-term message retention and replay

---

## Eventarc — GCP-Native Event Routing (Starting Capability)

Eventarc is Google Cloud's managed event routing service. This proposal scopes the initial Definity use case as **GCS onFinalize triggers routed to GKE HTTP endpoints** — the simplest, most concrete starting point.

Eventarc natively supports 90+ GCP event sources and multiple destination types (Cloud Run, Workflows, Cloud Functions 2nd gen, HTTP endpoints). These represent a natural growth path once the GCS trigger pattern is established and proven at Definity.

→ [Full Eventarc event sources and destinations](https://cloud.google.com/eventarc/docs/event-providers-targets)

---

### Initial Scope — GCS Trigger to GKE

How it works:

1. SDP DAG drops a file to a PI-owned GCS bucket
2. GCS fires `onFinalize` — Eventarc routes GCS metadata to GKE HTTP endpoint
3. GKE endpoint validates, deduplicates (`bucket + objectName + generation`), returns `202` immediately
4. Processing happens asynchronously — no Kafka broker, no connector, no SDK required
5. On delivery failure: exponential backoff retry → Pub/Sub DLQ → Cloud Storage Subscription → `FAILED_PAYLOADS` bucket → Cloud Monitoring alert to prod support

---

### Authentication

Eventarc uses GCP IAM service accounts for authenticated delivery:

- Dedicated Eventarc service account with OIDC token attached to each delivery request
- Destination validates token via GCP IAM — no API key or custom auth required
- Private GKE delivery via VPC-native networking — no public endpoint or external IP needed

→ [Eventarc authentication documentation](https://cloud.google.com/eventarc/docs/authentication)

---

### Known Limitations

| | |
|---|---|
| ✗ | **Bucket-level trigger only** — `onFinalize` fires for every object in the bucket; folder/prefix filtering not supported at trigger level. Mitigation: dedicated PI-owned bucket per pipeline. |
| ✗ | **At-least-once delivery** — duplicate events possible; application-level deduplication required (`bucket + objectName + GCS generation` key). |
| ✗ | **Single consumer per trigger** — no native fan-out; multiple consumers require multiple triggers. |
| ✗ | **No long-term retention** — unlike Kafka, events not retained for replay; replay requires re-triggering the source event (re-uploading the file generates a new GCS generation). |
| ✗ | **Not designed for high throughput** — event notification service, not a streaming platform. |

→ [Eventarc quotas and limitations](https://cloud.google.com/eventarc/quotas)

---

### Pros

| | |
|---|---|
| ✓ | Near-zero cost — within GCP free tier for low-frequency workloads; no idle infrastructure cost |
| ✓ | GCP-native — direct GCS integration, no connector layer or additional hops |
| ✓ | Serverless — fully managed by Google; no broker or connector lifecycle to maintain |
| ✓ | Fast setup — Terraform provisioned by CLDENG; no intake process or proxy onboarding |
| ✓ | Simple consumer — standard GKE HTTP endpoint; no Kafka SDK or consumer group management |
| ✓ | Built-in retry — configurable exponential backoff, natively supported, no custom implementation |
| ✓ | Native DLQ — Pub/Sub DLQ with Cloud Storage Subscription; no Cloud Function needed |
| ✓ | Day 2 under CMS — within existing CLDENG support scope; no new team or on-call rotation |
| ✓ | Extensible — GCS trigger is the starting point; broader GCP event sources available as pattern matures |

### Best Fit Workloads

- Low-frequency GCS file triggers — weekly, monthly, or quarterly cadence
- GCP-native, intra-org pipelines with a single producer and consumer
- Simple event notification where broker overhead is not justified
- Cost-sensitive workloads where Kafka's fixed cost is disproportionate to value

---

## Decision Matrix

| Workload Characteristic | Kafka | Eventarc |
|---|---|---|
| Real-time or high-throughput events | ✅ Recommended | ❌ Not Recommended |
| Low-frequency triggers (weekly / quarterly) | 🟡 Possible | ✅ Recommended |
| Multiple consumers / fan-out | ✅ Recommended | ❌ Not Recommended |
| Single consumer / domain-specific trigger | 🟡 Possible | ✅ Recommended |
| Multi-cloud or cross-system integration | ✅ Recommended | ❌ Not Recommended |
| GCP-native, intra-org pipeline | 🟡 Possible | ✅ Recommended |
| Enterprise event backbone | ✅ Recommended | ❌ Not Recommended |
| GCS file ingestion trigger | 🟡 Possible | ✅ Recommended |
| Long-term message retention / replay | ✅ Recommended | ❌ Not Recommended |
| Cost-sensitive, low-volume workload | ❌ Not Recommended | ✅ Recommended |

---

## Reference Implementations

Both implementations use GCS `onFinalize` as the event source and GKE as the destination — the initial approved scope.

| Project | Event Source | Destination | Frequency | Cost | Status |
|---|---|---|---|---|---|
| **RAVEN** | GCS onFinalize (iClarify file) | ms-ss-pi-raven (GKE) | Weekly | $0 (free tier) | Non-prod live. ARB conditional June 2026. |
| **Earnix** | GCS onFinalize (KML boundary file) | ms-ss-pi-polygon-service (GKE) | Quarterly | $0 (free tier) | Follows RAVEN pattern. Reusable Terraform. |

---

## Frequently Asked Questions

### Reliability & Failure Handling

**Q: If a pod fails, does the event get lost?**

A: No. Kubernetes restarts failed pods automatically. The DLQ scenario only applies when the entire deployment is unreachable across all retry attempts — for example during a complete rolling restart or misconfiguration affecting all replicas.

**Q: Wouldn't a failed delivery just queue and be retried automatically?**

A: Yes — that is the primary recovery path. Eventarc retries failed deliveries with configurable exponential backoff. Transient failures are handled automatically. The DLQ is only engaged after all retry attempts are exhausted.

**Q: Should prod support be notified on every delivery failure?**

A: No. Alerts fire only when retries are exhausted and the event lands in the DLQ. Transient failures retry silently — alerting on every retry would create noise without value for low-frequency workloads.

**Q: What is the difference between recoverable and irrecoverable failures?**

A: Recoverable failures (pod restarts, transient network outages, rolling deployments) are handled automatically by Eventarc retry. Irrecoverable failures (corrupted files, persistent application errors) exhaust retries, land in DLQ, and require prod support action — either re-upload the corrected file or notify the data provider to re-deliver.

---

### Backoff & Retry

**Q: How is backoff handled? Custom-built or native?**

A: Natively supported by Eventarc — no custom implementation required. Configurable exponential backoff (minimum delay, maximum delay, retry count) set at the Eventarc trigger level via Terraform.

---

### Day 2 Operations & CI/CD

**Q: Who owns Day 2 for Pub/Sub DLQ and Eventarc infrastructure?**

A: Existing CMS/CLDENG scope — Eventarc, Pub/Sub, and Cloud Storage Subscription are fully managed GCP services. No new support team, on-call rotation, or tooling required. Confirmed by CLDENG during infrastructure planning.

**Q: Is there a CI/CD pipeline for Eventarc? Is it centrally maintained?**

A: Yes. Reusable Terraform modules for Eventarc, Pub/Sub DLQ, and Cloud Storage Subscription are authored by CLDENG as part of RAVEN delivery. Available org-wide upon RAVEN non-prod completion — no project-specific CI/CD needed.

---

### Security & Networking

**Q: How does Eventarc authenticate to GKE? Does GKE need a public endpoint?**

A: No public exposure required. Eventarc authenticates using a GCP service account with an OIDC token on each request. For private GKE services, delivery uses VPC-native networking — no public load balancer or external IP needed.

---

### Pattern Consistency

**Q: Does approving Eventarc mean the org will have too many solutions for the same problem?**

A: No. The decision matrix defines a specific, unambiguous workload profile for Eventarc: low-frequency GCS triggers, single consumer, GCP-native, intra-org. Kafka remains the standard for everything else. One well-governed addition — not proliferation.

---

## Growth Path

The GCS trigger pattern is the starting point. Eventarc's broader capabilities become available as the pattern matures at Definity:

- **Phase 1 (current):** GCS `onFinalize` → GKE. Low-frequency file ingestion pipelines.
- **Phase 2:** Broader GCP event sources — Cloud Audit Logs, Pub/Sub, Firebase — as teams identify low-frequency intra-org use cases.
- **Phase 3:** Review existing Kafka implementations against the decision matrix to identify workloads that could reduce cost by switching to Eventarc.

*Phases 2 and 3 are not prerequisites for pattern approval — they represent the natural growth path once Phase 1 is proven.*

---

## Reference Documentation

- → [Eventarc overview](https://cloud.google.com/eventarc/docs/overview)
- → [Supported event sources and destinations](https://cloud.google.com/eventarc/docs/event-providers-targets)
- → [Authentication model](https://cloud.google.com/eventarc/docs/authentication)
- → [Creating triggers](https://cloud.google.com/eventarc/docs/creating-triggers)
- → [Retry and delivery guarantees](https://cloud.google.com/eventarc/docs/delivery)
- → [Quotas and limitations](https://cloud.google.com/eventarc/quotas)

---

## Recommendation

Approve Eventarc as a complementary integration pattern in the Definity API Integration Pattern Library, starting with:

- **GCS `onFinalize` triggers routed to GKE HTTP endpoints**
- **Low-frequency intra-org pipelines — weekly, monthly, or quarterly cadence**
- **Single consumer workloads where Kafka's throughput, fan-out, and multi-cloud capabilities are not required**

Kafka remains the enterprise standard for all other event-driven integration scenarios. These are complementary patterns — not competing ones.

---

*Bashkar Sampath | Manager, Technology | TS – Personal Insurance Technology | Definity | June 2026*
