/*
Copyright 2026 The Volcano Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package k8s

import (
	"errors"

	fwk "k8s.io/kube-scheduler/framework"
)

// ErrDRACandidateAllocationUnavailable reports that Filter's per-node
// AllocationResult cannot be read through a supported API.
//
// On k8s.io/kubernetes v1.36.1 the result lives in package-private state in
// pkg/scheduler/framework/plugins/dynamicresources:
//
//   - dynamicresources.go: stateKey ("DynamicResources"), stateData.nodeAllocations,
//     nodeAllocation.allocationResults, and unexported getStateData. Filter writes
//     the map. Score and Reserve read it inside that package. Score only turns the
//     chosen FirstAvailable subrequest into a preference score.
//   - nodeallocatabledynamicresources.go: ExtractPodNodeAllocatableResourceClaimStatus
//     is the only exported CycleState reader. It returns
//     []core.NodeAllocatableResourceClaimStatus, not device allocations.
//   - dra_manager.go: claimTracker.GetPendingAllocation is the post-Reserve pending
//     claim. GatherAllocatedState is the cluster-wide allocated device set. Neither
//     is the candidate result for one node, and neither lists eligible devices.
//
// Do not reflect that CycleState, copy structured.Allocator, or infer devices by
// counting ResourceSlice entries. binpack.dra.weight stays unimplemented until a
// read-only contract exists. See docs/design/dra-score-design.md.
var ErrDRACandidateAllocationUnavailable = errors.New("dra candidate allocation is not readable on k8s v1.36.1: dynamicresources.stateData.nodeAllocations is unexported")

// ReadDRACandidateAllocations is the placeholder for the read-only contract
// device-utilization binpack needs. It does not inspect state.
func ReadDRACandidateAllocations(_ fwk.CycleState) error {
	return ErrDRACandidateAllocationUnavailable
}
