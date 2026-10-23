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
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	k8sframework "k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/dynamicresources"
)

func TestReadDRACandidateAllocationsUnavailable(t *testing.T) {
	state := k8sframework.NewCycleState()
	err := ReadDRACandidateAllocations(state)
	if !errors.Is(err, ErrDRACandidateAllocationUnavailable) {
		t.Fatalf("expected unavailable candidate allocation, got %v", err)
	}

	// The only exported CycleState reader does not return AllocationResult.
	// An empty state therefore yields no node-allocatable claim status, which
	// is not a device-utilization input.
	got := dynamicresources.ExtractPodNodeAllocatableResourceClaimStatus(klog.Background(), state, "node-a")
	if len(got) != 0 {
		t.Fatalf("expected no node-allocatable claim status, got %#v", got)
	}
	var _ []v1.NodeAllocatableResourceClaimStatus = got
}
