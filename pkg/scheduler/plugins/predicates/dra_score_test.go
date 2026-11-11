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

package predicates

import (
	"context"
	"sort"
	"sync"
	"testing"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8sframework "k8s.io/kube-scheduler/framework"
	schedframework "k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/dynamicresources"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/plugins/util/k8s"
	"volcano.sh/volcano/pkg/scheduler/plugins/util/nodescore"
)

func TestInitPluginDRAScoreWeightAndSharedInstance(t *testing.T) {
	tests := []struct {
		name       string
		args       framework.Arguments
		wantScore  bool
		wantWeight int
	}{
		{name: "default weight", wantScore: true, wantWeight: 1},
		{name: "explicit weight", args: framework.Arguments{draScoreWeightKey: 3}, wantScore: true, wantWeight: 3},
		{name: "zero disables score only", args: framework.Arguments{draScoreWeightKey: 0}, wantScore: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ResetVolumeBindingPluginForTest()
			pp := draPredicatesForTest(t, tt.args)
			pp.InitPlugin()

			filter := pp.FilterPlugins[dynamicresources.Name]
			if filter == nil {
				t.Fatal("DRA filter was not registered")
			}
			if any(pp.PreFilterPlugins[dynamicresources.Name]) != any(filter) ||
				any(pp.ReservePlugins[dynamicresources.Name]) != any(filter) ||
				any(pp.PreBindPlugins[dynamicresources.Name]) != any(filter) {
				t.Fatal("DRA PreFilter, Filter, Reserve and PreBind are not the same instance")
			}
			if _, preScore := pp.PreScorePlugins[dynamicresources.Name]; preScore {
				t.Fatal("DRA does not implement PreScore and must not be registered as one")
			}
			score := pp.ScorePlugins[dynamicresources.Name]
			if tt.wantScore {
				if any(score) != any(filter) {
					t.Fatal("DRA Score is not the same instance as Filter")
				}
				if pp.ScoreWeights[dynamicresources.Name] != tt.wantWeight {
					t.Fatalf("expected DRA score weight %d, got %d", tt.wantWeight, pp.ScoreWeights[dynamicresources.Name])
				}
				return
			}
			if score != nil {
				t.Fatal("weight 0 should not register DRA Score")
			}
			if _, ok := pp.ScoreWeights[dynamicresources.Name]; ok {
				t.Fatal("weight 0 should not record a DRA score weight")
			}
			if _, ok := pp.ReservePlugins[dynamicresources.Name]; !ok {
				t.Fatal("weight 0 must keep DRA Reserve")
			}
			if _, ok := pp.PreBindPlugins[dynamicresources.Name]; !ok {
				t.Fatal("weight 0 must keep DRA PreBind")
			}
		})
	}
}

func TestBatchNodeOrderUsesFilterCycleStateAndCandidates(t *testing.T) {
	plugin := &recordingScorePlugin{score: 10}
	state := schedframework.NewCycleState()
	pp := &PredicatesPlugin{
		ScorePlugins: scorePlugins(plugin),
		ScoreOrder:   []string{plugin.Name()},
		ScoreWeights: map[string]int{plugin.Name(): 4},
	}

	nodeA := testNodeInfo("node-a")
	nodeB := testNodeInfo("node-b")
	scores, err := pp.BatchNodeOrder(
		&api.TaskInfo{Pod: &apiv1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod", Namespace: "ns"}}},
		[]k8sframework.NodeInfo{nodeA, nodeB},
		state,
	)
	if err != nil {
		t.Fatalf("BatchNodeOrder returned an error: %v", err)
	}
	if !plugin.sameState || plugin.state != state {
		t.Fatal("Score did not receive the CycleState passed to BatchNodeOrder")
	}
	gotNodes := append([]string(nil), plugin.nodes...)
	sort.Strings(gotNodes)
	if len(gotNodes) != 2 || gotNodes[0] != "node-a" || gotNodes[1] != "node-b" {
		t.Fatalf("expected only candidate nodes node-a and node-b, got %v", gotNodes)
	}
	if scores["node-a"] != 40 || scores["node-b"] != 40 {
		t.Fatalf("expected weight to scale the raw score to 40, got %v", scores)
	}
	if _, scored := scores["node-c"]; scored {
		t.Fatal("a node outside the candidate set was scored")
	}
}

func TestBatchNodeOrderReturnsScoreAndNormalizeErrors(t *testing.T) {
	task := &api.TaskInfo{Pod: &apiv1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod", Namespace: "ns"}}}
	nodes := []k8sframework.NodeInfo{testNodeInfo("node-a"), testNodeInfo("node-b")}

	t.Run("score error drops the batch", func(t *testing.T) {
		plugin := &recordingScorePlugin{status: k8sframework.NewStatus(k8sframework.Error, "score failed")}
		pp := &PredicatesPlugin{
			ScorePlugins: scorePlugins(plugin),
			ScoreOrder:   []string{plugin.Name()},
			ScoreWeights: map[string]int{plugin.Name(): 1},
		}
		scores, err := pp.BatchNodeOrder(task, nodes, schedframework.NewCycleState())
		if err == nil || scores != nil {
			t.Fatalf("expected score error without partial scores, got scores=%v err=%v", scores, err)
		}
	})

	t.Run("normalize error drops the batch", func(t *testing.T) {
		plugin := &recordingScorePlugin{
			score: 1,
			extensions: &fixedNormalizeStatus{
				status: k8sframework.NewStatus(k8sframework.Error, "normalization failed"),
			},
		}
		pp := &PredicatesPlugin{
			ScorePlugins: scorePlugins(plugin),
			ScoreOrder:   []string{plugin.Name()},
			ScoreWeights: map[string]int{plugin.Name(): 1},
		}
		scores, err := pp.BatchNodeOrder(task, nodes, schedframework.NewCycleState())
		if err == nil || scores != nil {
			t.Fatalf("expected normalize error without partial scores, got scores=%v err=%v", scores, err)
		}
	})
}

func TestDynamicResourcesScoreWithoutFilterState(t *testing.T) {
	ResetVolumeBindingPluginForTest()
	pp := draPredicatesForTest(t, nil)
	pp.InitPlugin()
	scores, err := pp.BatchNodeOrder(
		&api.TaskInfo{Pod: &apiv1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod", Namespace: "ns"}}},
		[]k8sframework.NodeInfo{testNodeInfo("node-a")},
		schedframework.NewCycleState(),
	)
	if err == nil || scores != nil {
		t.Fatalf("missing Filter state must fail the batch, got scores=%v err=%v", scores, err)
	}
}

func TestDisabledDynamicResourcesScoreIsZero(t *testing.T) {
	// Kubernetes v1.36 locks DynamicResourceAllocation on, so predicates cannot
	// build this disabled instance through the feature gate. The zero value is
	// the plugin New returns when the gate is off, and its Score stays 0.
	plugin := &dynamicresources.DynamicResources{}
	pod := &apiv1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod", Namespace: "ns"}}
	scores, err := nodescore.CalculatePluginScore(
		dynamicresources.Name,
		plugin,
		schedframework.NewCycleState(),
		pod,
		[]k8sframework.NodeInfo{testNodeInfo("node-a")},
		1,
	)
	if err != nil {
		t.Fatalf("disabled DRA score returned an error: %v", err)
	}
	if scores["node-a"] != 0 {
		t.Fatalf("expected score 0 when DRA is disabled, got %v", scores["node-a"])
	}
}

func draPredicatesForTest(t *testing.T, args framework.Arguments) *PredicatesPlugin {
	t.Helper()
	pp := New(args).(*PredicatesPlugin)
	pp.enabledPredicates = predicateEnable{dynamicResourceAllocationEnable: true}
	client := k8sfake.NewSimpleClientset()
	pp.Handle = k8s.NewFramework(
		map[string]k8sframework.NodeInfo{},
		k8s.WithClientSet(client),
		k8s.WithInformerFactory(informers.NewSharedInformerFactory(client, 0)),
	)
	return pp
}

func scorePlugins(plugin *recordingScorePlugin) map[string]nodescore.BaseScorePlugin {
	return map[string]nodescore.BaseScorePlugin{plugin.Name(): plugin}
}

type recordingScorePlugin struct {
	mu         sync.Mutex
	score      int64
	status     *k8sframework.Status
	extensions k8sframework.ScoreExtensions
	state      k8sframework.CycleState
	sameState  bool
	nodes      []string
	scored     bool
}

func (p *recordingScorePlugin) Name() string { return "recording-score" }

func (p *recordingScorePlugin) Score(_ context.Context, state k8sframework.CycleState, _ *apiv1.Pod, nodeInfo k8sframework.NodeInfo) (int64, *k8sframework.Status) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.scored {
		p.state = state
		p.sameState = true
		p.scored = true
	} else if p.state != state {
		p.sameState = false
	}
	p.nodes = append(p.nodes, nodeInfo.Node().Name)
	if p.status != nil {
		return 0, p.status
	}
	return p.score, nil
}

func (p *recordingScorePlugin) ScoreExtensions() k8sframework.ScoreExtensions { return p.extensions }

type fixedNormalizeStatus struct {
	status *k8sframework.Status
}

func (e *fixedNormalizeStatus) NormalizeScore(context.Context, k8sframework.CycleState, *apiv1.Pod, k8sframework.NodeScoreList) *k8sframework.Status {
	return e.status
}

func testNodeInfo(name string) k8sframework.NodeInfo {
	nodeInfo := schedframework.NewNodeInfo()
	nodeInfo.SetNode(&apiv1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}})
	return nodeInfo
}
