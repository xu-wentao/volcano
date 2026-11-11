# Capacity Plugin DRA Support Design and Implementation Review

> This review covers only the capacity plugin's queue quota. It is not the design for DRA node scoring or device-utilization binpack. Scoring is described in [dra-score-design.md](./dra-score-design.md). The quota fixes listed here are not implemented by the DRA score change.

## Review Scope

This review covers:

- `docs/design/capacity-dra-support.md`
- DRA request summary generation:
  - `pkg/scheduler/cache/cache.go`
  - `pkg/scheduler/cache/event_handlers.go`
  - `pkg/scheduler/api/job_info.go`
- Queue admission, allocation, hierarchical queues, and shared Claim accounting in the capacity plugin:
  - `pkg/scheduler/plugins/capacity/capacity.go`
- DRA display in Queue `status.allocated`:
  - `pkg/scheduler/framework/session.go`
  - `pkg/scheduler/framework/session_dra_queue_status.go`
- Unit tests and e2e:
  - `pkg/scheduler/cache/cache_dra_test.go`
  - `pkg/scheduler/plugins/capacity/capacity_test.go`
  - `pkg/scheduler/framework/session_dra_queue_status_test.go`
  - `test/e2e/dra/dra_quota_test.go`
- The Kubernetes v1.36.1 DRA API and the scheduler `DynamicResources` implementation.

The review uses four kinds of conclusion:

- **correctness bug**: current behavior violates a quota semantic that has already been stated.
- **implementation gap**: the design claims support, but the code does not yet connect the corresponding capacity mechanism.
- **design/contract problem**: the semantic must be made explicit before the implementation can be judged correct.
- **explicit limitation**: acceptable, but it must be rejected explicitly or exposed clearly. It must not pass silently.

## Overall Conclusion

Expressing DRA logical quota in the Queue `ResourceList`, while leaving physical allocation to Kubernetes DRA, is a sound split of responsibility. Narrowing the first version to an `ExactCount` hard limit per `DeviceClass` can also be a usable minimum.

The current design document describes the actual capability too broadly. The existing implementation does not deliver these core promises:

- DRA has the same `capability`, `deserved`, and `guarantee` semantics as ordinary resources.
- Every DRA request is constrained by a strict queue hard limit.
- A shared `ResourceClaim` is counted once in admission, allocation, hierarchical ancestor queues, and Queue Status.
- A DRA inqueue reservation survives across scheduling sessions.
- Consumable capacity means the capacity Kubernetes actually consumes.
- The root queue's DRA total can be derived automatically from cluster state.

The loop that actually closes is closer to this:

> A Pod explicitly references a `ResourceClaim`, or uses a Pod-level `ResourceClaimTemplate`. The Claim contains only `Exactly + ExactCount` requests. Inside a single queue, allocated Tasks are checked against a hard limit using the `DeviceClass` declared in the request and the raw capacity quantity. Claims with the same name are partially deduplicated.

Even inside that narrow range there are correctness bugs: shared Claim admission is charged more than once, inqueue state is lost across sessions, hierarchical ancestor queues miss usage when they are rebuilt, and `capability: 0` is treated as unlimited. The current implementation should not be described as strict DRA queue quota, or as fully equivalent to ordinary capacity resources.

## Design Promises Compared with the Current Implementation

| Capability | Documented promise | Current implementation | Conclusion |
|---|---|---|---|
| `capability` device count | hard upper bound | checked separately by `checkDRAAllocatable` | Partly usable, with bypasses and boundary bugs |
| `capability` capacity dimension | hard upper bound | checks the raw quantity in the Claim | Contract is unclear. This is not actual `ConsumedCapacity` |
| `deserved` | soft share / borrow / reclaim | parsed only. DRA allocated does not participate in share or reclaim | Not implemented |
| `guarantee` | minimum protected share | parsed only. DRA usage does not participate in protection or reclaim | Not implemented |
| direct Claim | supported | supported on the normal path | An informer race can drop the Task |
| Pod-level ClaimTemplate | supported | supported for a resolved Claim | The normal path mostly holds |
| shared Claim | once per Claim | partial dedup during allocation | enqueue and cross-queue/ancestor rebuild are inconsistent |
| `FirstAvailable` | deferred | skipped silently | Bypasses the hard quota |
| `allocationMode: All` | deferred | skipped silently | Bypasses the hard quota |
| DRA Extended Resource | not stated | never enters the summary | v1.36 beta capability is uncovered |
| Kubernetes Workload/PodGroup Claim | not stated | native PodGroup cycle state is not wired | v1.36 alpha capability is uncovered |
| root DRA total | cluster state or explicit configuration | only explicit Queue configuration | Document and implementation disagree |

## Findings by Severity

### 4.1 Critical: unsupported requests are skipped silently and can bypass the hard quota

Type: **correctness bug and unsafe handling of a limitation**

Evidence:

- The design defines `capability` as a hard upper bound: `docs/design/capacity-dra-support.md:42-52`.
- `FirstAvailable` and `allocationMode: All` are only described as "not the main target": `docs/design/capacity-dra-support.md:164-172`.
- The cache prints a warning and then `continue`s for both request kinds:
  - `pkg/scheduler/cache/cache.go:1973-1977`
  - `pkg/scheduler/cache/cache.go:1982-1985`
- If every request in a Claim is one of those kinds, `buildTaskDRAInfo` returns nil: `pkg/scheduler/cache/cache.go:2007-2024`.
- capacity allows the Task when `taskDRA == nil`: `pkg/scheduler/plugins/capacity/capacity.go:246-249`.

Impact:

- A queue configured with `deviceclass/gpu.example.com: "1"` can be bypassed by requesting the same class of device with `FirstAvailable` or `All`. Volcano treats the request as having no DRA quota consumption.
- In a mixed Claim, only the supported request is charged. The remaining requests still bypass quota.
- This is a silent fail-open. It does not match the safety boundary of a hard upper bound.

Recommendation:

- If the first version does not support these modes, reject them explicitly when the queue has the related DRA quota enabled, and record a diagnosable event on the PodGroup or Job.
- The cache must not compress "unsupported" into "no DRA request." It must at least keep the unsupported reason and the DeviceClass involved.
- When `FirstAvailable` is supported later, charging must follow the subrequest selected by Kubernetes Filter. Do not add every alternative, and do not always take the first one.
- The quantity for `All` depends on the matched device set. It cannot be derived from the Claim Spec alone. Use the actual allocation result, or state clearly that it is unsupported.

### 4.2 Critical: `deserved` / `guarantee` are not implemented and can distort queue ordering

Type: **implementation gap and correctness bug**

Evidence:

- The document says the three fields have exactly the same semantics as ordinary resources: `docs/design/capacity-dra-support.md:42-52` and `174-181`.
- DRA-specific `deserved` and `guarantee` are parsed into `draQuotaAttr`: `pkg/scheduler/plugins/capacity/capacity.go:103-115` and `181-190`.
- Nothing reads `dra.deserved` or `dra.guarantee` except initialization and clone. The DRA check uses only `dra.capability`: `pkg/scheduler/plugins/capacity/capacity.go:246-305`.
- Queue share still reads only ordinary `attr.allocated` and ordinary `attr.deserved`: `pkg/scheduler/plugins/capacity/capacity.go:1817-1843`.
- A DRA Task updates only `attr.dra.allocated`. It does not write the DRA key into ordinary `attr.allocated.ScalarResources`: `pkg/scheduler/plugins/capacity/capacity.go:351-409` and `924-1003`.

Impact:

- A queue that configures only DRA `deserved` carries the DRA key in ordinary `attr.deserved`, while the corresponding ordinary allocated stays 0 forever. Share can remain 0 and the queue can receive a high priority that does not match its real DRA usage.
- DRA usage does not participate in queue borrow, reclaim, preemption, or guarantee protection.
- The `deserved` and `guarantee` values in the examples do not have the effect on real DRA scheduling that the document claims.

Recommendation:

- The minimum deliverable version should promise and allow only DRA `capability`. Remove DRA `deserved` / `guarantee` examples from the design and user guide for now, and reject those keys if necessary.
- Claim equivalence with ordinary resources only after DRA count and capacity are wired into queue dominant share, reclaim victim selection, guarantee, and hierarchy calculation.
- Do not stuff DRA keys into an ordinary `Resource` only to "reuse" the semantics, unless the scalar of the same name is fully maintained for allocation, request, inqueue, and elastic.

### 4.3 High: a shared Claim is still charged once per Pod at Job enqueue

Type: **correctness bug**

Evidence:

- The document promises that the same shared Claim is counted once: `docs/design/capacity-dra-support.md:134-141`.
- The allocation path does deduplicate with a per-queue refcount: `pkg/scheduler/plugins/capacity/capacity.go:321-409`.
- `JobInfo.GetMinDRAResources` aggregates each Task's `DRAResreq` and does not use `ResourceClaimKeys` or `ResourceClaimDRAResreq`: `pkg/scheduler/api/job_info.go:1492-1584`.
- Without `TaskMinAvailable`, it adds Tasks one by one: `pkg/scheduler/api/job_info.go:1525-1552`. With a task role, it multiplies one Task's request by that role's minimum replica count: `pkg/scheduler/api/job_info.go:1560-1579`.
- The shared Claim e2e has two replicas but deliberately uses `minAvailable=1`: `test/e2e/dra/dra_quota_test.go:147-169`. It does not cover the double charge at `minAvailable=2`.

Impact:

- Two gang members reference the same count=1 Claim, with queue capability=1 and minAvailable=2. Job enqueue computes 2 and rejects the Job, while allocation and Queue Status show 1.
- The same quota has different semantics at admission and allocation.

Recommendation:

- `GetMinDRAResources` must aggregate by Claim identity. It must not aggregate per Task and then multiply by replica count.
- Claim identity should prefer UID over `namespace/name`, so a same-name recreation after deletion is distinct.
- For a shared Claim, define the charging domain explicitly:
  - leaf queue: once per Claim per leaf queue;
  - ancestor/root: once per Claim inside the same ancestor subtree;
  - when a Claim is shared across leaf queues, whether every leaf is charged must be written into the contract.

### 4.4 High: DRA inqueue reservation is not rebuilt across scheduling sessions

Type: **correctness bug**

Evidence:

- When a Job is first admitted, `AddJobEnqueuedFn` adds `GetMinDRAResources` to `dra.inqueue`: `pkg/scheduler/plugins/capacity/capacity.go:781-813`.
- A new session initializes `dra.inqueue` as an empty map: `pkg/scheduler/plugins/capacity/capacity.go:181-188`.
- Session open rebuilds ordinary `attr.inqueue` from existing Jobs, but there is no corresponding `updateDRAInqueue`:
  - non-hierarchical queues: `pkg/scheduler/plugins/capacity/capacity.go:1124-1144`
  - hierarchical queues: `pkg/scheduler/plugins/capacity/capacity.go:1271-1288`
- Allocating a Task increases `dra.allocated` and does not subtract the covered minimum demand from `dra.inqueue`: `pkg/scheduler/plugins/capacity/capacity.go:924-963`.
- The Task allocatable check explicitly ignores inqueue, while the Job enqueue check counts both allocated and inqueue: `pkg/scheduler/plugins/capacity/capacity.go:1646-1655` and `1697-1719`.
- The enqueue check depends on `allocated + inqueue + request`: `pkg/scheduler/plugins/capacity/capacity.go:1711-1719`.

Impact:

- A DRA Job that is already Inqueue, but not yet allocated because no physical device is available, reserves quota only in the session where it was admitted.
- The next session forgets that reservation. A later Job can continue to enter Inqueue, which is inconsistent with ordinary capacity resources.
- Inside one session, after a Job is admitted and some of its Tasks are allocated, `dra.allocated` and the full `dra.inqueue` are both retained. In the normal action order the Task hard check does not read inqueue. If enqueue is checked again in the same session, the covered part is counted twice.
- The hard check on a single Task allocation may still prevent a real overrun, but queue admission, gang reservation, and fairness are already distorted.

Recommendation:

- Session open must rebuild DRA inqueue the same way it rebuilds ordinary resources.
- The rebuilt value should be the part of the Job's minimum DRA demand that is not yet covered by an allocated Claim, deduplicated by Claim, so the same usage is not present in both allocated and inqueue.
- When a Job moves from inqueue to partially allocated inside a session, maintain that same uncovered remainder. Do not keep the full minDRA while also increasing allocated.
- Add a unit test that crosses two sessions. Calling `updateDRAInqueue` directly to construct state is not enough.

### 4.5 High: hierarchical queue rebuild can miss or double-count ancestor DRA usage

Type: **correctness bug**

Evidence:

- Session open counts allocated Tasks only when the leaf `attr.dra != nil`: `pkg/scheduler/plugins/capacity/capacity.go:1255-1263`.
- It then propagates only the leaf's numeric delta to ancestors that configure capability for the same DeviceClass: `pkg/scheduler/plugins/capacity/capacity.go:1296-1318`.
- The propagated value is an aggregated number. The Claim refcount is not propagated with it.
- During the session, Allocate and Deallocate events call `addTaskDRAAllocated` / `removeTaskDRAAllocated` directly on every ancestor and keep the refcount: `pkg/scheduler/plugins/capacity/capacity.go:924-1003`.

Impact:

- If the parent queue configures DRA capability and the child leaf does not, rebuilding leaf usage yields nil. The parent cannot see DRA allocations that already existed before the session started.
- When different leaf queues reference the same Claim, the numeric delta is added again on the parent, while the root recalculation of Queue Status deduplicates by Claim key.
- Incremental state inside the session, rebuilt state in the next session, and `status.allocated` can give different answers.

Recommendation:

- Do not derive ancestor Claim usage from a leaf's aggregated delta.
- Session open should walk allocated Tasks and use the same Claim-identity refcount logic on the leaf and on each ancestor.
- Add tests for "only the parent has quota," "two leaves share one Claim," and "the session is reopened."

### 4.6 High: a count-only capability unexpectedly rejects a Claim that requests capacity

Type: **correctness bug / missing contract**

Evidence:

- Once consumable capacity is enabled, `checkDRAAllocatable` walks every dimension in the request.
- If the queue does not configure that dimension, it returns false: `pkg/scheduler/plugins/capacity/capacity.go:282-300`.

Impact:

- The queue only wants to limit device count and configures `deviceclass/gpu.example.com: "4"`.
- The Claim requests the same DeviceClass and also requests `memory: 8Gi`.
- Even though the queue declared no memory quota, the Claim is rejected because capability has no memory dimension.

This disagrees with the usual ResourceList semantic that an unconfigured quota key does not limit that dimension. It also disagrees with the code path that allows an unconfigured DeviceClass.

Recommendation:

- An unconfigured capacity dimension should mean unlimited.
- If allowlist semantics are intended, the design must say so and provide a separate switch. Do not infer them from the presence of a count key.

### 4.7 High: `deviceclass/<class>: "0"` actually means unlimited

Type: **correctness bug**

Evidence:

- The count hard limit is checked only when `capability.Count > 0`: `pkg/scheduler/plugins/capacity/capacity.go:273-280`.
- Queue DRA keys are not validated as integer, non-negative, or zero: `pkg/scheduler/plugins/capacity/capacity.go:142-179`.

Impact:

- An administrator sets count capability to 0, which intuitively forbids the queue from using the device.
- The check is skipped, so the queue is unlimited.
- A negative value also disables the check. A fractional value rounded by `Quantity.Value()` is not intuitive.

Recommendation:

- 0 must be a valid hard zero.
- Reject negative and non-integer counts.
- "Not configured" and "configured as 0" must be distinguished by whether the map key exists, not by the sign of the value.

### 4.8 High: a direct ResourceClaim informer race can drop a Pod from the cache

Type: **correctness bug**

Evidence:

- When the Pod arrives and the direct Claim is not yet in the ResourceClaim informer cache, `buildTaskDRAInfo` returns an ordinary error: `pkg/scheduler/cache/cache.go:1950-1965`.
- `addPod` calls `addTask` and then resyncs only for `pendingDRAResourceClaimError`. Today only the template path constructs that error: `pkg/scheduler/cache/event_handlers.go:270-280`.
- The ordinary error path only calls `resyncTask(pi)` and does not add the Task to `sc.Jobs`: `pkg/scheduler/cache/event_handlers.go:298-300`.
- `processResyncTask` must then find the Task in `sc.Jobs`. If it cannot, it Forgets the work: `pkg/scheduler/cache/cache.go:1224-1263`.

Impact:

- If a Pod ADD arrives before the direct ResourceClaim informer ADD, the Pod may never enter the scheduler cache.
- When the ResourceClaim arrives later, there is no Task here to retry.
- Existing tests only check that the build function returns an error. They do not cover recovery in `addPod`: `pkg/scheduler/cache/cache_dra_test.go:227-237`.

Recommendation:

- NotFound and informer-not-synced for a direct Claim should use the same pending path as the template race.
- Other read errors should still fail fast, so a real fault is not swallowed.
- Add a direct Claim test symmetric to `TestAddPodWithUnresolvedResourceClaimTemplateCachesTaskForResync`.

### 4.9 High: the capacity dimension key cannot express a common fully-qualified DRA capacity name

Type: **design/API problem**

Evidence:

- The design encodes the key as `<dim>.deviceclass/<DeviceClass>`: `docs/design/capacity-dra-support.md:33-40`.
- Kubernetes DRA `QualifiedName` allows and encourages a third-party capacity to use `<domain>/<name>`, for example `dra.example.com/memory`.
- Substituting that dimension into the current format produces `dra.example.com/memory.deviceclass/gpu.example.com`, which contains two `/` characters and cannot be a Kubernetes `ResourceName`.
- The parser itself allows `/` before the separator, but the object is first constrained by the API ResourceName format: `pkg/scheduler/plugins/capacity/capacity.go:142-163`.

Impact:

- The current key reliably supports only a simple dimension with no domain.
- The design claims general consumable-capacity support, but a third-party fully-qualified capacity cannot be configured.
- Concatenating the dimension and DeviceClass is also limited by ResourceName length.

Recommendation:

- For the first version, restrict the key to an unqualified dimension and write that limit into the API documentation; or
- redesign a reversible and legal key encoding; or
- if both readability and complete expression must be kept, re-evaluate the non-goal of "never add a DRA-specific queue field," so the value is not forced into `ResourceList` with a fragile string encoding.

### 4.10 High: the declared capacity request is not the capacity Kubernetes actually consumes

Type: **design contract problem. If the feature claims a physical consumption quota, this is a correctness bug.**

Evidence:

- The cache multiplies the raw per-device quantity in the Claim Spec by count: `pkg/scheduler/cache/cache.go:1905-1921` and `1991-2005`.
- The document also defines logical usage as "2 × 8Gi = 16Gi": `docs/design/capacity-dra-support.md:143-154`.
- In Kubernetes v1.36:
  - `requestPolicy` can round the requested quantity up;
  - when a dimension is not requested explicitly, the actual default consumption may be the whole device capacity;
  - the final value is recorded in `results[*].consumedCapacity` of the allocation result.

Impact:

- Queue status and the hard check can be lower than the device's actual consumption.
- The same Claim can produce different actual consumed capacity on different candidate devices.
- Partitionable and multi-allocation devices are especially likely to diverge.

Recommendation:

- Choose one contract and state it:
  1. **requested-capacity quota**: limit only the raw quantity the user declared in the Claim Spec. This is simpler to implement, but it must be renamed and it must be explicit that this is not physical usage.
  2. **consumed-capacity quota**: use the allocation result after Kubernetes selects a node, and handle Reserve, rollback, and session restore. Reading the Claim Spec is not enough.
- The first version should choose 1, and should list requestPolicy, default consumption, and partitionable devices as cases that do not guarantee a physical upper bound.

### 4.11 Medium: Kubernetes v1.36 DRA Extended Resource is not counted in DRA quota

Type: **implementation gap / undeclared boundary**

Evidence:

- `buildTaskDRAInfo` returns immediately when `pod.Spec.ResourceClaims` is empty: `pkg/scheduler/cache/cache.go:1924-1927`.
- Kubernetes v1.36 `DRAExtendedResource` is beta. Native `DynamicResources` constructs or reuses a special ResourceClaim for the Pod's extended resource during PreFilter and Filter.

Impact:

- A DRA workload that enters through the extended-resource compatibility path does not consume `deviceclass/*` quota.
- It may still consume ordinary extended-resource queue quota, but that is a different configuration key and cannot replace the design promise.

Recommendation:

- State in the support matrix that a DRA Extended Resource is not managed by DeviceClass quota.
- Longer term, reuse the actual Claim and request mapping produced by native DynamicResources, so the cache does not derive it a second time.

### 4.12 Medium: the root queue "automatic cluster total" is not implemented, and a unique DeviceClass total is hard to compute

Type: **document and implementation disagree**

Evidence:

- The document says the cluster-wide DRA total can come from actual cluster state: `docs/design/capacity-dra-support.md:156-162`.
- The capacity plugin does not read `ResourceSlice` or `DeviceClass`.
- The DRA total comes only from the Queue `capability` / `deserved` / `guarantee` parser: `pkg/scheduler/plugins/capacity/capacity.go:142-192`.

There is a more basic design problem: several DeviceClass selectors can match the same physical device, so an automatic total summed by DeviceClass is not necessarily additive and not necessarily unique.

Recommendation:

- The current version should be explicit: **the root DRA capability is only what an administrator configures.**
- Do not promise a DeviceClass total computed automatically from ResourceSlice.
- If an automatic total is needed later, first define the rules for overlapping DeviceClasses, node-local and global devices, partitionable devices, and an incomplete ResourceSlice generation.

### 4.13 Medium: Queue Status and enforcement use different enable conditions and data semantics

Type: **implementation consistency problem**

Evidence:

- Queue Status unconditionally merges DRA from TaskInfo at session close: `pkg/scheduler/framework/session.go:501-556`.
- Status merge deduplicates by Claim key: `pkg/scheduler/framework/session_dra_queue_status.go:31-108`.
- Capacity enforcement is controlled by plugin arguments: `pkg/scheduler/plugins/capacity/capacity.go:412-430`.
- The TaskInfo capacity summary is also controlled by the global Kubernetes feature gate: `pkg/scheduler/cache/cache.go:1924-1932`.

Impact:

- Status can show DRA usage while the capacity plugin does not enforce it.
- A plugin argument can enable consumable capacity while the global feature gate is off, so the capacity summary is empty and the request is allowed silently.
- Status shows the Claim Spec request quantity, not the actual consumption in the allocation result.

Recommendation:

- Document the enable conditions for observation and enforcement separately.
- A plugin argument must not create a half-enabled state that contradicts the global feature gate. At least report an error at session open.
- The Queue Status field name or documentation should say that the value is requested quota usage.

## Boundaries That Are Undefined or Uncovered

### Shared Claim charging domain

The current design only says "once per Claim." It does not answer:

- One Claim referenced by several Jobs in the same queue: once, or once per Job?
- One Claim referenced by two leaf queues: once per leaf, or owned by only one queue?
- Should a common ancestor and the root count once or twice?
- After a Claim is deleted and recreated with the same name, how are the old Task and the new Task distinguished?

Use the Claim UID as the identity, and define the charging domain for leaf, ancestor, and root.

### Minimum DRA demand of a heterogeneous gang

Without `TaskMinAvailable`, `GetMinDRAResources` sorts Tasks that have DRA by name and takes the first `MinAvailable` of them: `pkg/scheduler/api/job_info.go:1525-1552`.

If a Job also has Tasks without DRA, or Tasks with different DRA requests, the Task combination that can actually form the gang is not unique. The current algorithm is not necessarily a lower bound, and it does not necessarily match the combination the scheduler will schedule first. The design needs to say whether:

- a DRA Job must provide per-task-role minAvailable;
- admission uses a conservative upper bound; or
- enforcement happens only at actual Task allocation, and an uncertain combination is not used for Job enqueue.

### A Claim whose allocation already exists

An ExactCount Spec is immutable, so device count can usually be accounted from the Spec. Real capacity consumption may be known only in `status.allocation.devices.results[*].consumedCapacity`. The design needs to say:

- how much an unallocated Claim reserves from its request;
- whether Reserve and PreBind reconcile the value;
- how scheduling rollback undoes it;
- how a scheduler restart restores it from Claim Status.

### Overlapping DeviceClasses

A DeviceClass is a selector, not a driver or an exclusive resource pool. Two classes can match the same device. Charging logical quota by the class in the request is feasible. Adding quotas of different classes and calling the sum a physical total is not.

### Kubernetes v1.36 Workload ResourceClaims

The v1.36 alpha `DRAWorkloadResourceClaims` feature supports a native Workload or PodGroup Claim and lets the same PodGroup scheduling cycle reuse a pending allocation. Current Volcano TaskInfo builds the quota summary only from Pod `spec.resourceClaims`. It also does not define a native PodGroup Claim identity and lifecycle for capacity accounting.

This capability can be deferred, but it must be listed in the unsupported matrix. "ResourceClaimTemplate is supported" must not be read as support for a PodGroup-level template.

### Admin access, taints, binding conditions, and partitionable devices

These are still undefined:

- whether admin access consumes ordinary queue quota;
- whether device taint and toleration affect the admission estimate;
- how an inqueue reservation is held while a binding condition stays pending for a long time;
- whether the count of a partitionable device is a device, a share, or an allocation-result count.

The first version can defer them, but it must say whether the behavior is fail-open, fail-closed, or "not allowed together with DRA quota."

## Limitations That Are Reasonable

These boundaries are reasonable. The capacity plugin does not need to copy the Kubernetes allocator:

- It does not place devices on nodes.
- It does not solve capacity fragmentation on a single device.
- DeviceClass quota is queue policy. It does not replace the ResourceSlice availability check.
- The first version supports only `Exactly + ExactCount`.
- The root total is configured explicitly by an administrator.

The problem is not that the scope is narrow. The problem is that the current implementation silently treats an out-of-scope request as 0, and the document still claims full `capability` / `deserved` / `guarantee` equivalence.

## Recommended Revision

### Phase 1: narrow the contract and fix hard capability

Make the first-version contract explicit:

- Support only an explicit Pod ResourceClaim and a Pod-level ResourceClaimTemplate.
- Support only `Exactly + ExactCount`.
- Count quota is a non-negative integer. 0 means the queue may not use the device.
- Define capacity quota as **raw requested capacity quota**.
- An unconfigured capacity dimension is unlimited.
- Promise only `capability`. Do not promise DRA `deserved` / `guarantee` yet.
- When the related DRA quota is configured, `FirstAvailable`, `All`, DRA Extended Resource, and a native Workload Claim are rejected or reported as unsupported. They are not skipped silently.
- Root DRA capability accepts only an administrator's explicit configuration.

This phase also fixes:

1. the direct Claim informer race;
2. Job minimum-resource dedup for a shared Claim;
3. DRA inqueue rebuild across sessions;
4. hierarchy rebuild by Claim identity;
5. semantic labels for Queue Status and enforcement.

### Phase 2: decide later whether to implement DRA fair share

Implement DRA `deserved` / `guarantee` only when they are actually required. At a minimum, wire them together into:

- queue share and order;
- reclaim and preemption;
- guarantee;
- hierarchical deserved and guarantee;
- allocated, request, inqueue, and elastic;
- metrics and Queue Status.

Parsing the configuration fields is not enough unless DRA usage is placed on every path above.

### Phase 3: allocation-aware capacity

If the requirement is to limit Kubernetes' actual `ConsumedCapacity`, reuse the allocation result from native DynamicResources. Do not keep extending the Spec summary in the cache:

- The concrete device, subrequest, and adjusted capacity are known only after Filter.
- Reserve and Unreserve are the pending-allocation lifecycle.
- After PreBind, Claim Status is the durable fact.
- A scheduler restart restores state from allocated and pending Claims.

This expands the implementation substantially. It should be a separate feature from "raw requested quota."

## Test Recommendations

### Unit tests that must be added

1. Under a DRA hard quota, `FirstAvailable` is not treated as a zero request.
2. Under a DRA hard quota, `allocationMode: All` is not treated as a zero request.
3. A mixed Claim with one supported request and one unsupported request is not partially charged and then allowed.
4. A count capability of 0 rejects a positive request.
5. A negative or fractional count capability is rejected.
6. A count-only capability allows a Claim that also requests capacity.
7. When only one capacity dimension is configured, an unconfigured dimension is unlimited.
8. After a direct Claim informer NotFound, the Task stays in the cache and can resync.
9. Two minAvailable Tasks share one Claim, and Job enqueue counts it once.
10. The same Claim referenced by different task roles is still counted once.
11. An Inqueue DRA Job keeps its reservation across two sessions.
12. A partially allocated gang is not charged in both allocated and inqueue.
13. A parent-only DRA capability sees the child's existing allocation.
14. When two child queues share a Claim, the parent deduplicates according to the defined charging domain.
15. Incremental state during a session matches the result rebuilt in the next session.
16. Queue Status, leaf enforcement, and ancestor enforcement report the same usage for the same set of Tasks.

### Consumable capacity tests

1. Request count multiplied by raw requested capacity.
2. API and key behavior of a fully-qualified dimension.
3. requestPolicy rounding, with a clear proof of whether status is requested or consumed.
4. Default consumption when capacity is not requested explicitly.
5. One Claim with several requests and several DeviceClasses.
6. A partitionable or multi-allocation device is rejected explicitly or handled according to the contract.

### E2E gaps

Existing e2e covers a direct Claim, a Pod-level template, a simple shared Claim, a count hard limit, a capacity hard limit, and recovery after deletion. The basic happy path is useful. It still needs:

- a shared Claim with `replicas=2, minAvailable=2`, so the test does not bypass enqueue dedup the way the current test does;
- inqueue reservation after a scheduler session or restart;
- hierarchical parent-only quota;
- an explicit failure event for an unsupported request;
- borrow and reclaim e2e for queue `deserved` / `guarantee` only after those semantics are actually implemented;
- a Queue Status check that compares the full DRA key set, not only the subset match of `queueAllocatedContains`: `test/e2e/dra/dra_quota_test.go:396-417`.

## Recommended Design Conclusion

What the design document needs now is a narrow contract that can be verified, not a longer support list:

> The first version of the Volcano capacity plugin provides a per-DeviceClass queue hard capability for an explicit Pod ResourceClaim that uses `Exactly + ExactCount`. Consumable capacity means the raw quantity in the Claim Spec. It does not mean Kubernetes' final `ConsumedCapacity`. An unsupported DRA request is not silently treated as zero. DRA `deserved`, `guarantee`, a native Workload Claim, Extended Resource, `FirstAvailable`, `All`, and partitionable devices are not promised yet.

Fix hard capability, shared Claims, and session/hierarchy consistency before discussing fair share and actual consumed capacity. That is the smallest scope, and it is the least likely to create a second incomplete DRA allocator.

## References

- [Kubernetes Dynamic Resource Allocation](https://kubernetes.io/docs/concepts/scheduling-eviction/dynamic-resource-allocation/)
- [Kubernetes v1.36 ResourceClaim API](https://kubernetes.io/docs/reference/kubernetes-api/resource/resource-claim-v1/)
- [Kubernetes v1.36 DRA updates](https://kubernetes.io/blog/2026/05/07/kubernetes-v1-36-dra-136-updates/)
- [Kubernetes v1.36.1 DynamicResources scheduler plugin](https://github.com/kubernetes/kubernetes/blob/v1.36.1/pkg/scheduler/framework/plugins/dynamicresources/dynamicresources.go)
- [Kubernetes v1.36.1 DRA API types](https://github.com/kubernetes/kubernetes/blob/v1.36.1/staging/src/k8s.io/api/resource/v1/types.go)
