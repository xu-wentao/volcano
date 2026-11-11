# Volcano DRA Node Scoring Design

## Summary

Volcano is missing DRA scoring, and that gap is two separate problems:

1. **Semantic completeness.** Volcano already reuses the kube-scheduler `dynamicresources` plugin for `PreFilter`, `Filter`, `Reserve`, and `PreBind`, but it does not call the plugin's `Score` and `NormalizeScore`. That drops the priority semantics of a `FirstAvailable` request.
2. **Scheduling policy.** Native kube-scheduler DRA Score does not bin-pack by device utilization. If Volcano wants to reduce device fragmentation, it needs a separate DRA binpack policy.

This design lands in two phases:

- **Phase 1 (required first).** Wire kube-scheduler's native DRA `Score` / `NormalizeScore` through the predicates plugin. This phase can be implemented directly. It is the goal of [Volcano issue #5329](https://github.com/volcano-sh/volcano/issues/5329).
- **Phase 2 (has a prerequisite).** Compute device utilization from the actual allocation that DRA Filter produces for each candidate node. The Kubernetes plugin stores that allocation in private CycleState, and Volcano has no supported read API. Until that API exists, do not implement a heuristic DRA binpack.

Guessing `DeviceClass -> driver`, caching ResourceSlice counts for the session, and manually adjusting usage with `AllocateFunc` / `DeallocateFunc` cannot be the implementation basis. Those approaches produce wrong results with multiple drivers, selectors, shared Claims, `AllNodes`, multi-slice generations, consumable capacity, and gang rollback.

## Current Baseline

The semantic baseline is Kubernetes `v1.36.1`, as declared by the current `go.mod` and `vendor/modules.txt`. After a later Kubernetes upgrade, the extension-point guard must reconfirm which `dynamicresources` interfaces exist and which of them Volcano adapts.

The current Volcano path is:

| Stage | Current implementation | Conclusion |
| --- | --- | --- |
| Pod/Claim summary | `buildTaskDRAInfo` in `pkg/scheduler/cache/cache.go` | Suitable only as a capacity quota summary. It does not describe the physical allocation. |
| PreFilter/Filter | predicates reuses `dynamicresources.DynamicResources` | Wired |
| Score/NormalizeScore | predicates does not register DRA Score | Missing |
| Reserve/Unreserve/PreBind | predicates reuses the native DRA plugin | Wired |
| CPU/memory and other binpack | `pkg/scheduler/plugins/binpack/binpack.go` | Handles only scalar resources on `TaskInfo.Resreq` |

`task.DRAResreq` currently keeps only `DeviceClass`, count, and capacity dimensions. It skips requests such as `FirstAvailable` and `allocationMode: All`. It also does not keep the request selector, toleration, constraint, request or subrequest name, or the final device identity. It can answer "how much did this queue logically request" for quota accounting. It cannot answer "which devices will this node actually receive."

## Kubernetes DRA Scoring Semantics

### Resource identity and accessibility

A DRA device identity is `(driver, pool, device)`, not `DeviceClass`. A `DeviceClass` contains selectors. It can match one or more drivers, and there is no general one-to-one `DeviceClass -> driver` mapping.

A ResourceSlice pool can:

- be split across multiple slices;
- describe one complete snapshot through `generation` and `resourceSliceCount` together;
- express accessibility with `nodeName`, `nodeSelector`, `allNodes`, or per-device node selection;
- express partitionable or consumable capacity through shared counters;
- be affected by DeviceTaintRule, device status, and request tolerations.

Counting `len(slice.Spec.Devices)` by class or driver is therefore not a reliable node capacity.

### Native scheduling path

The Kubernetes `v1.36.1` `dynamicresources` plugin works in this order:

```text
PreFilter
  ├─ resolve Pod / PodGroup ResourceClaims
  ├─ read DeviceClass, ResourceSlice, allocated devices, and pending allocation
  └─ create the structured allocator
        ↓
Filter(node)
  ├─ run real device selection for that node
  └─ store that node's AllocationResult in the same CycleState
        ↓
Score(node)
  └─ read that node's AllocationResult and see which FirstAvailable subrequest was chosen
        ↓
NormalizeScore
  └─ normalize into [0, MaxNodeScore]
        ↓
Reserve(node)
  ├─ take the AllocationResult of the selected node
  └─ record the pending allocation through DRAManager
        ↓
PreBind
  └─ persist the ResourceClaim allocation
```

Native Score means "prefer the earlier subrequest in a `FirstAvailable` list." It does not mean "prefer the node with higher device utilization." For an ordinary Claim that contains only ExactCount, the native Score is usually 0.

## Design Goals

### Goals

1. Keep Volcano consistent with the current kube-scheduler DRA Score semantics.
2. Score must reuse the CycleState written by PreFilter and Filter in the same scheduling cycle.
3. Call the plugin's own `ScoreExtensions()` and `NormalizeScore`.
4. Define a safe data boundary and a minimal first version for a future DRA binpack.
5. Leave the capacity plugin's queue quota semantics unchanged.

### Non-Goals

- Copy the Kubernetes structured allocator into Volcano.
- Parse CEL text to guess a driver or a device set.
- Use `task.DRAResreq` as a substitute for the physical device allocation.
- Support consumable capacity, partitionable devices, or shared-capacity binpack in the first version.
- Use a weight of 0 to mean spread. A weight of 0 only disables that score term.

## Phase 1: Native DRA Score

### Registration

DRA Score must keep running on the same `dynamicresources.DynamicResources` instance that the predicates plugin already creates. binpack and nodeorder must not create a second instance.

```text
predicates.OnSessionOpen
  ├─ PrePredicateFn  -> DynamicResources.PreFilter
  ├─ PredicateFn     -> DynamicResources.Filter
  ├─ BatchNodeOrderFn
  │    ├─ DynamicResources.Score(each candidate)
  │    └─ DynamicResources.NormalizeScore(all candidates)
  └─ AllocateFunc    -> DynamicResources.Reserve
```

The call must use `ssn.GetCycleState(task.UID)`. The per-node AllocationResult written by Filter and the state read by Score must be the same object. `BatchNodeOrderFn` receives only the nodes that already passed Filter, which is the complete candidate set `NormalizeScore` needs.

### Score helper

`nodescore.BaseScorePlugin` currently requires a plugin to implement both `PreScorePlugin` and `ScorePlugin`. Kubernetes DRA implements only `ScorePlugin`. The shared score helper needs to:

1. Accept `fwk.ScorePlugin`.
2. Call `PreScore` only when the plugin also implements `fwk.PreScorePlugin`.
3. Read `plugin.ScoreExtensions()` directly.
4. Run `NormalizeScore` when `ScoreExtensions()` is non-nil.
5. Check and return a failed NormalizeScore status. The current helper ignores that failure.
6. After normalization, validate `[MinNodeScore, MaxNodeScore]` and then apply the Volcano weight.

This supports DRA, which has no PreScore, and removes the duplicated `EmptyNormalizer` or hand-specified normalizer logic maintained by each caller.

### Configuration

Add an argument next to `volumebinding.weight` in the predicates arguments:

```yaml
tiers:
- plugins:
  - name: predicates
    arguments:
      predicate.DynamicResourceAllocationEnable: true
      dynamicresources.filterTimeoutSeconds: 10
      dynamicresources.weight: 1
```

Semantics:

- The default is `1`, matching the common weight in the kube-scheduler default profile.
- `0` disables DRA Score only. It does not disable DRA Filter, Reserve, or PreBind.
- A negative value is invalid configuration. Initialization must report it explicitly. It must not be interpreted as spread.

This weight affects only the `FirstAvailable` preference. The native DRA Score of an ExactCount request is 0, so changing the weight does not produce device binpack.

### Error handling

- If PreScore, Score, or NormalizeScore returns a failure, that BatchNodeOrder call must return the error and must not use partial node scores.
- An out-of-range score keeps the existing score helper validation and returns an error.
- No DRA Claim, no candidate AllocationResult, or a plugin disabled by its feature gate returns 0. Those normal cases must not be recorded as scheduling errors.
- DeviceClass, ResourceSlice, or ResourceClaim lister errors must not be swallowed.

## Phase 2: DRA Device-Utilization Binpack

### Prerequisite

Filter has already computed the real AllocationResult for each candidate node, but `dynamicresources.stateData.nodeAllocations` is a private field inside the Kubernetes package. Volcano cannot read it safely without copying the allocator or using reflection.

Before implementation, a supported read-only contract is required. The priority is:

1. Ask the Kubernetes DRA plugin to expose a read-only candidate-allocation or scoring extension.
2. If upstream does not accept that, keep a minimal, testable hook in Volcano's Kubernetes dependency adapter and tie it explicitly to the Kubernetes version.
3. Reject copying the structured allocator, reading private CycleState, using reflection, or guessing the allocation by scanning again.

The contract must provide at least:

- Claim UID and request/subrequest identity.
- The `(driver, pool, device)` actually selected for the candidate node.
- Whether the result reuses an existing allocation or creates a new one.
- The device set that is truly eligible for this request and node, or an equivalent capacity statistic.
- For a future consumable-capacity version: consumed, pending, and newly requested capacity.

Until that contract exists, phase 2 is **not implementable**. It does not block phase 1.

### First supported scope

Even after the candidate-allocation contract exists, the first version supports only:

- `allocationMode: ExactCount`;
- exclusive, whole-device allocation;
- node-local devices expressed by `ResourceSlice.spec.nodeName`;
- complete pool data with a consistent generation;
- a new allocation that Filter has already produced successfully.

In the following cases the whole task's DRA binpack returns 0. Native DRA logic still decides feasibility and the `FirstAvailable` preference:

- `FirstAvailable`, so binpack does not override the user's explicit subrequest priority;
- `allocationMode: All`;
- a Claim that is already allocated or shared by multiple Pods;
- consumable capacity, shared counters, partitionable devices, and ShareID;
- `allNodes`, `nodeSelector`, and per-device node selection;
- an implicit Claim created for a DRA Extended Resource, unless the candidate-allocation contract exposes it explicitly;
- an incomplete pool snapshot, an inconsistent generation, or any identity that cannot be confirmed;
- one task that mixes supported and unsupported DRA requests, so scoring only part of the requests does not bias the result.

This is an intentional safe downgrade. Filter and Reserve keep full DRA correctness. The missing piece is only an optional ordering preference.

### Score formula

For candidate node `n`, compute each eligible device domain `d` touched by the candidate allocation:

- `T_d(n)`: total exclusive devices this request can use on the node.
- `U_d(n)`: how many of those devices are allocated or marked pending by DRAManager.
- `R_d(n)`: how many devices this candidate allocation will add.

```text
util_d(n) = (U_d(n) + R_d(n)) / T_d(n)

draScore(n) =
    100 * sum(R_d(n) * util_d(n)) / sum(R_d(n))
```

A domain is scorable only when `0 < U_d + R_d <= T_d`. `R_d` is the weight, so a request that allocates several devices is not treated as equal to a single-device request.

The final binpack score is:

```text
finalBinpackScore(n) =
    currentScalarBinpackScore(n)
    + binpack.dra.weight * draScore(n)
```

`currentScalarBinpackScore` already includes the existing `binpack.weight`. The DRA score itself stays in `[0, 100]` and is multiplied by `binpack.dra.weight` once. Do not multiply by the weight and then divide by the same weight.

Suggested configuration:

```yaml
- name: binpack
  arguments:
    binpack.weight: 10
    binpack.cpu: 5
    binpack.memory: 1
    binpack.dra.weight: 0  # phase 2 default off; a positive value enables it
```

Phase 2 should use `BatchNodeOrderFn` because it needs the full candidate set and the same CycleState in one call. The existing CPU and memory NodeOrderFn can stay.

### Gang, Pipeline, and rollback

DRA resource state must follow the native DRAManager:

- `Reserve` records the selected AllocationResult as a pending allocation.
- A later task's PreFilter and Filter see that pending allocation.
- `Unreserve` releases the corresponding pending state on failure or rollback.
- A persisted allocation is expressed by ResourceClaim status.

The binpack plugin must not maintain `used++` / `used--` with `AllocateFunc` / `DeallocateFunc`. Volcano's `Statement.Pipeline` also calls `AllocateFunc`. Dry-run discard and recover, preempt, reclaim, and shared Claims fire different events again. The number of events is not the number of physical device allocations.

The current Pipeline also triggers the predicates DRA Reserve. Whether that lifecycle matches the long-term meaning of Pipeline is a separate predicates/statement question. A score cache must not "correct" it.

Kubernetes `v1.36` has dedicated scheduling-cycle state for a PodGroup sharing a pending ResourceClaim. A Volcano gang or PodGroup is not naturally equivalent to Kubernetes `Pod.spec.schedulingGroup`. Before claiming support for "multiple gang tasks sharing an unallocated Claim," add integration coverage. An ordinary Volcano Allocate event does not replace that semantic.

### Concurrency and cache

Volcano scores multiple nodes concurrently. The DRA Score path may read only this task's CycleState and a thread-safe DRA manager snapshot. Score must not modify a session-level map.

Do not add a second session-level ResourceSlice cache. The Kubernetes DRA manager and allocator already handle informer updates, pool generation, device taints, and pending allocation. A copied cache is redundant and becomes stale when objects change inside the session.

## Relationship to Capacity DRA

The two features have different jobs, but their data and lifecycle are not "fully orthogonal":

| Dimension | Capacity DRA | DRA Score |
| --- | --- | --- |
| Goal | Logical queue quota | Ordering candidate nodes |
| Main input | Claim request summary | Filter's actual candidate AllocationResult |
| Resource grain | `deviceclass/<name>` and capacity dimension | `(driver, pool, device)` and actual capacity |
| Shared Claim | The quota layer should count each Claim UID once | Reuse the allocation and do not add physical usage again |
| Correctness boundary | Does not decide physical allocatability | Does not decide whether the queue may use the resource |

`task.DRAResreq` can continue to serve capacity. It must not become the physical device source for phase 2. Both features can be enabled together. Neither feature may assume that the other has validated every DRA request semantic.

## Expected Changes

### Phase 1

| File | Change |
| --- | --- |
| `pkg/scheduler/plugins/util/nodescore/score_helper.go` | Support a ScorePlugin without PreScore, use the plugin's own normalizer, and check the status |
| `pkg/scheduler/plugins/util/nodescore/score_helper_test.go` | Cover optional PreScore, NormalizeScore success and failure, and out-of-range scores |
| `pkg/scheduler/plugins/predicates/helper.go` | Add `dynamicresources.weight` |
| `pkg/scheduler/plugins/predicates/predicates.go` | Register the same DRA instance as a ScorePlugin |
| `pkg/scheduler/plugins/predicates/predicates_test.go` | Cover registration, weight, CycleState reuse, and the candidate set |
| DRA e2e test | Verify ordering when different nodes hit different FirstAvailable subrequests |
| Kube plugin interface guard test | After a Kubernetes dependency upgrade, detect a new extension point that has not been handled |

`nodeorder` also uses the shared score helper. If the helper drops the external normalizer argument, simplify `scorePluginEntry` in the same change and run the nodeorder regression tests.

### Phase 2

The phase 2 file set depends on where the read-only candidate-allocation contract lives. Before that contract is reviewed, do not promise "only `binpack.go` and `binpack_test.go` change," and do not add a DRA session cache first.

## Test Matrix

### Phase 1 required tests

| Scenario | Expected result |
| --- | --- |
| No DRA Claim | DRA Score is 0 on every node |
| ExactCount | Node order is unchanged |
| Two nodes hit different FirstAvailable subrequests | The node that hits the earlier subrequest scores higher |
| Several Claims each have FirstAvailable | Scores accumulate and normalize correctly |
| Only one node remains after Filter | NormalizeScore succeeds, and Reserve uses that node's allocation |
| ScoreExtensions is nil | NormalizeScore is not called |
| NormalizeScore returns an error | BatchNodeOrder returns the error and does not use a partial result |
| `dynamicresources.weight: 0` | Score is disabled only. Filter, Reserve, and PreBind still run |
| Schedule the next task after Reserve | The next task sees the pending allocation |
| Statement discard/recover | The pending allocation is Unreserved |
| Gang tasks sharing an unallocated Claim | Do not claim support before schedulingGroup integration is done |

### Additional phase 2 required tests

| Scenario | Expected result |
| --- | --- |
| Two node-local pools have different utilization | Binpack chooses the higher post-allocation utilization |
| One request allocates several devices | `R_d` weights the score correctly |
| A DeviceClass matches several drivers | Use the actual AllocationResult and do not guess the driver |
| An allocated or shared Claim | DRA binpack is 0 and the Claim is not counted again |
| FirstAvailable, All, or consumable capacity | The whole task falls back to 0 |
| Incomplete pool generation | Fall back to 0 and record an observable reason |
| Concurrent Score | The race test shows no shared-map read/write contention |
| Pipeline, discard, preempt, reclaim | There is no independent cache drift |

## Release and Compatibility

1. Merge phase 1 and the extension-point guard first, and restore semantic consistency with kube-scheduler.
2. On a small DRA cluster, verify FirstAvailable, Reserve/Unreserve, and gang behavior.
3. Review the read-only candidate AllocationResult contract separately.
4. Release phase 2 with `binpack.dra.weight: 0` by default.
5. Expand to global devices, consumable capacity, or partitionable devices only after the supported cases pass e2e and race tests.

## References

- [Volcano issue #5329: Support DRA Score/NormalizeScore](https://github.com/volcano-sh/volcano/issues/5329)
- [Kubernetes v1.36.1 DynamicResources plugin](https://github.com/kubernetes/kubernetes/blob/v1.36.1/pkg/scheduler/framework/plugins/dynamicresources/dynamicresources.go)
- [Kubernetes DRA concepts](https://kubernetes.io/docs/concepts/scheduling-eviction/dynamic-resource-allocation/)
- [Kubernetes DeviceClass API](https://kubernetes.io/docs/reference/kubernetes-api/resource/device-class-v1/)
- [Kubernetes ResourceSlice API](https://kubernetes.io/docs/reference/kubernetes-api/resource/resource-slice-v1/)
- [Kubernetes ResourceClaim API](https://kubernetes.io/docs/reference/kubernetes-api/resource/resource-claim-v1/)
- [Volcano capacity DRA design](./capacity-dra-support.md)
- [Capacity DRA support review](./capacity-dra-support-review.md) (quota; unrelated to the scoring in this document)

## Implementation Status on Kubernetes v1.36.1

This section records the code checked when this design was implemented, so the baseline above is not mistaken for a gap that no longer exists.

### Phase 1

`predicates` already registers `PreFilter`, `Filter`, `Reserve`, `PreBind`, and `Score` on the same `*dynamicresources.DynamicResources` instance. `BatchNodeOrder` reads `ssn.GetCycleState(task.UID)`, the same state used by PrePredicate and Predicate. `nodescore.CalculatePluginScore` calls the plugin's own `ScoreExtensions().NormalizeScore` and returns an error for the whole batch on failure. It checks `[MinNodeScore, MaxNodeScore]` after normalization and then multiplies by the Volcano weight. `DynamicResources` has no `PreScore`. Callers invoke `PreScore` only when the plugin also implements `PreScorePlugin`.

The configuration added here is `dynamicresources.weight`:

- The default is `1`.
- `0` does not register DRA Score. Filter, Reserve, and PreBind still run.
- A negative value or a non-integer fails initialization. It must not be interpreted as spread.

Native Score still expresses only the `FirstAvailable` preference. The native score of ExactCount is usually 0. Changing this weight does not produce device-utilization binpack. Kubernetes v1.36 locks `DynamicResourceAllocation` on. When the plugin itself has `enabled == false`, it still returns 0 for every node, and `NormalizeScore` returns nil. That nil status is treated as success and is not recorded as a scheduling error. If Filter did not write CycleState, Score failure fails the whole BatchNodeOrder.

The extension-point guard is `pkg/scheduler/plugins/predicates/dra_extension_points_test.go`. If Kubernetes adds an unclassified exported method to `DynamicResources`, the test fails until that method is explicitly marked adapted or ignored. The points currently left unwired are `PreEnqueue`, `EventsToRegister`, `PostFilter`, `SignPod`, and `PreBindPreFlight`.

### Phase 2 remains blocked

`k8s.io/kubernetes@v1.36.1` has no read-only contract Volcano can use for Filter's per-node `AllocationResult`. The relevant symbols are in `pkg/scheduler/framework/plugins/dynamicresources`:

| Symbol | Location | Why it cannot be the binpack input |
| --- | --- | --- |
| `stateKey` | `dynamicresources.go` | Package-private `StateKey`, value `"DynamicResources"` |
| `stateData.nodeAllocations` | `dynamicresources.go` | Unexported. Filter writes it. Score and Reserve read it only inside the package |
| `nodeAllocation.allocationResults` | `dynamicresources.go` | Unexported `[]resourceapi.AllocationResult` |
| `getStateData` | `dynamicresources.go` | Unexported |
| `(*DynamicResources).Score` / `computeScore` | `dynamicresources.go` | Adds a score from the chosen `FirstAvailable` index and does not expose device identity |
| `ExtractPodNodeAllocatableResourceClaimStatus` | `nodeallocatabledynamicresources.go` | The only exported CycleState reader. It returns `[]v1.NodeAllocatableResourceClaimStatus`, not a device allocation |
| `(*claimTracker).GetPendingAllocation` | `dra_manager.go` | Pending allocation after Reserve, not the Filter result of each candidate node |
| `(*claimTracker).GatherAllocatedState` | `dra_manager.go` | Cluster-wide allocated devices. It has neither this candidate allocation nor the eligible device set |

That is not enough to obtain the Claim UID, request/subrequest, `(driver, pool, device)`, new versus reused allocation, and eligible device count required by section 6.1.

`ReadDRACandidateAllocations` in `pkg/scheduler/plugins/util/k8s/dra_candidate_allocation.go` marks this gap. It always returns `ErrDRACandidateAllocationUnavailable`. It does not reflect CycleState, copy the structured allocator, or scan ResourceSlices.

Until upstream or Volcano's Kubernetes adapter provides a testable read-only contract:

- `binpack.dra.weight` is not implemented.
- `task.DRAResreq` and `len(slice.Spec.Devices)` are not treated as node device capacity.
- binpack does not maintain `used++` / `used--` with `AllocateFunc` / `DeallocateFunc`.

Capacity queue quota is described in `capacity-dra-support.md` and `capacity-dra-support-review.md`. It is outside the implementation scope of this design.
