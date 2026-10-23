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

package nodescore

import (
	"context"
	"strings"
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	fwk "k8s.io/kube-scheduler/framework"
	k8sframework "k8s.io/kubernetes/pkg/scheduler/framework"

	"volcano.sh/volcano/pkg/scheduler/api"
)

type fakeScorePlugin struct {
	name        string
	scores      map[string]int64
	scoreStatus *fwk.Status
	extensions  *fakeScoreExtensions
}

func (p *fakeScorePlugin) Name() string {
	return p.name
}

func (p *fakeScorePlugin) Score(_ context.Context, _ fwk.CycleState, _ *v1.Pod, nodeInfo fwk.NodeInfo) (int64, *fwk.Status) {
	if p.scoreStatus != nil {
		return 0, p.scoreStatus
	}
	return p.scores[nodeInfo.Node().Name], nil
}

func (p *fakeScorePlugin) ScoreExtensions() fwk.ScoreExtensions {
	if p.extensions == nil {
		return nil
	}
	return p.extensions
}

type fakeScoreExtensions struct {
	calls     int
	normalize func(fwk.NodeScoreList)
	status    *fwk.Status
}

func (e *fakeScoreExtensions) NormalizeScore(_ context.Context, _ fwk.CycleState, _ *v1.Pod, scores fwk.NodeScoreList) *fwk.Status {
	e.calls++
	if e.normalize != nil {
		e.normalize(scores)
	}
	return e.status
}

func TestNodeInfosForCandidateNodes(t *testing.T) {
	nodeA := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}}
	nodeB := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-b"}}
	nodeC := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-c"}}
	nodeD := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-d"}}

	k8sNodeInfoA := k8sframework.NewNodeInfo()
	k8sNodeInfoA.SetNode(nodeA)
	k8sNodeInfoB := k8sframework.NewNodeInfo()
	k8sNodeInfoB.SetNode(nodeB)
	k8sNodeInfoC := k8sframework.NewNodeInfo()
	k8sNodeInfoC.SetNode(nodeC)

	got := NodeInfosForCandidateNodes(
		[]*api.NodeInfo{api.NewNodeInfo(nodeA), nil, api.NewNodeInfo(nodeD), api.NewNodeInfo(nodeC)},
		map[string]fwk.NodeInfo{
			"node-a": k8sNodeInfoA,
			"node-b": k8sNodeInfoB,
			"node-c": k8sNodeInfoC,
		},
	)

	if len(got) != 2 {
		t.Fatalf("expected 2 candidate node infos, got %d", len(got))
	}
	if got[0].Node().Name != "node-a" {
		t.Fatalf("expected first node to be node-a, got %s", got[0].Node().Name)
	}
	if got[1].Node().Name != "node-c" {
		t.Fatalf("expected second node to be node-c, got %s", got[1].Node().Name)
	}
}

func TestCalculatePluginScore(t *testing.T) {
	tests := []struct {
		name           string
		extensions     *fakeScoreExtensions
		expectedScores map[string]float64
		expectedCalls  int
	}{
		{
			name: "skips normalization when score extensions are absent",
			expectedScores: map[string]float64{
				"node-a": 20,
				"node-b": 40,
			},
		},
		{
			name: "normalizes scores before applying weight",
			extensions: &fakeScoreExtensions{
				normalize: func(scores fwk.NodeScoreList) {
					scores[0].Score = 50
					scores[1].Score = 75
				},
			},
			expectedScores: map[string]float64{
				"node-a": 100,
				"node-b": 150,
			},
			expectedCalls: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plugin := &fakeScorePlugin{
				name:       "test-score-plugin",
				scores:     map[string]int64{"node-a": 10, "node-b": 20},
				extensions: tt.extensions,
			}

			scores, err := CalculatePluginScore(
				plugin.Name(),
				plugin,
				k8sframework.NewCycleState(),
				&v1.Pod{},
				testNodeInfos("node-a", "node-b"),
				2,
			)
			if err != nil {
				t.Fatalf("CalculatePluginScore returned an error: %v", err)
			}
			for nodeName, expectedScore := range tt.expectedScores {
				if score := scores[nodeName]; score != expectedScore {
					t.Errorf("expected score %v for %s, got %v", expectedScore, nodeName, score)
				}
			}
			if tt.extensions != nil && tt.extensions.calls != tt.expectedCalls {
				t.Errorf("expected NormalizeScore to be called %d times, got %d", tt.expectedCalls, tt.extensions.calls)
			}
		})
	}
}

func TestCalculatePluginScoreHonorsNilStatusAndScoreRange(t *testing.T) {
	nodeInfos := testNodeInfos("node-a")

	t.Run("nil score status is success", func(t *testing.T) {
		plugin := &fakeScorePlugin{
			name:   "nil-status",
			scores: map[string]int64{"node-a": fwk.MaxNodeScore},
		}
		scores, err := CalculatePluginScore(plugin.Name(), plugin, k8sframework.NewCycleState(), &v1.Pod{}, nodeInfos, 3)
		if err != nil {
			t.Fatalf("CalculatePluginScore returned an error: %v", err)
		}
		if scores["node-a"] != float64(fwk.MaxNodeScore*3) {
			t.Fatalf("expected weighted score %d, got %v", fwk.MaxNodeScore*3, scores["node-a"])
		}
	})

	t.Run("nil normalize status is success", func(t *testing.T) {
		plugin := &fakeScorePlugin{
			name:       "nil-normalize",
			scores:     map[string]int64{"node-a": 10},
			extensions: &fakeScoreExtensions{},
		}
		scores, err := CalculatePluginScore(plugin.Name(), plugin, k8sframework.NewCycleState(), &v1.Pod{}, nodeInfos, 1)
		if err != nil {
			t.Fatalf("CalculatePluginScore returned an error: %v", err)
		}
		if scores["node-a"] != 10 {
			t.Fatalf("expected score 10, got %v", scores["node-a"])
		}
	})

	t.Run("raw score above max fails before weight", func(t *testing.T) {
		plugin := &fakeScorePlugin{
			name:   "too-high",
			scores: map[string]int64{"node-a": fwk.MaxNodeScore + 1},
		}
		_, err := CalculatePluginScore(plugin.Name(), plugin, k8sframework.NewCycleState(), &v1.Pod{}, nodeInfos, 1)
		if err == nil || !strings.Contains(err.Error(), "invalid score") {
			t.Fatalf("expected invalid score error, got %v", err)
		}
	})

	t.Run("normalized score below min fails", func(t *testing.T) {
		plugin := &fakeScorePlugin{
			name:   "too-low",
			scores: map[string]int64{"node-a": 1},
			extensions: &fakeScoreExtensions{
				normalize: func(scores fwk.NodeScoreList) {
					scores[0].Score = fwk.MinNodeScore - 1
				},
			},
		}
		_, err := CalculatePluginScore(plugin.Name(), plugin, k8sframework.NewCycleState(), &v1.Pod{}, nodeInfos, 1)
		if err == nil || !strings.Contains(err.Error(), "invalid score") {
			t.Fatalf("expected invalid score error, got %v", err)
		}
	})
}

func TestCalculatePluginScoreReturnsScoreError(t *testing.T) {
	plugin := &fakeScorePlugin{
		name:        "failing-score",
		scoreStatus: fwk.NewStatus(fwk.Error, "score failed"),
	}
	_, err := CalculatePluginScore(plugin.Name(), plugin, k8sframework.NewCycleState(), &v1.Pod{}, testNodeInfos("node-a"), 1)
	if err == nil || !strings.Contains(err.Error(), "score failed") {
		t.Fatalf("expected score error, got %v", err)
	}
}

func TestRunPreScorePlugin(t *testing.T) {
	plugin := &preScoreAwarePlugin{}
	skip, err := RunPreScorePlugin(plugin, k8sframework.NewCycleState(), &v1.Pod{}, nil)
	if err != nil || skip {
		t.Fatalf("expected success without skip, got skip=%t err=%v", skip, err)
	}

	plugin.status = fwk.NewStatus(fwk.Skip)
	skip, err = RunPreScorePlugin(plugin, k8sframework.NewCycleState(), &v1.Pod{}, nil)
	if err != nil || !skip {
		t.Fatalf("expected skip, got skip=%t err=%v", skip, err)
	}

	plugin.status = fwk.NewStatus(fwk.Error, "pre-score failed")
	_, err = RunPreScorePlugin(plugin, k8sframework.NewCycleState(), &v1.Pod{}, nil)
	if err == nil || !strings.Contains(err.Error(), "pre-score failed") {
		t.Fatalf("expected pre-score error, got %v", err)
	}

	plugin.status = nil
	skip, err = RunPreScorePlugin(plugin, k8sframework.NewCycleState(), &v1.Pod{}, nil)
	if err != nil || skip {
		t.Fatalf("nil status should be success, got skip=%t err=%v", skip, err)
	}
}

type preScoreAwarePlugin struct {
	status *fwk.Status
}

func (p *preScoreAwarePlugin) Name() string { return "pre-score" }

func (p *preScoreAwarePlugin) PreScore(context.Context, fwk.CycleState, *v1.Pod, []fwk.NodeInfo) *fwk.Status {
	return p.status
}

func TestCalculatePluginScoreReturnsNormalizeError(t *testing.T) {
	extensions := &fakeScoreExtensions{
		status: fwk.NewStatus(fwk.Error, "normalization failed"),
	}
	plugin := &fakeScorePlugin{
		name:       "test-score-plugin",
		scores:     map[string]int64{"node-a": 10},
		extensions: extensions,
	}

	_, err := CalculatePluginScore(
		plugin.Name(),
		plugin,
		k8sframework.NewCycleState(),
		&v1.Pod{},
		testNodeInfos("node-a"),
		1,
	)
	if err == nil || !strings.Contains(err.Error(), "normalization failed") {
		t.Fatalf("expected normalization error, got %v", err)
	}
	if extensions.calls != 1 {
		t.Errorf("expected NormalizeScore to be called once, got %d", extensions.calls)
	}
}

func testNodeInfos(names ...string) []fwk.NodeInfo {
	nodeInfos := make([]fwk.NodeInfo, 0, len(names))
	for _, name := range names {
		nodeInfo := k8sframework.NewNodeInfo()
		nodeInfo.SetNode(&v1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}})
		nodeInfos = append(nodeInfos, nodeInfo)
	}
	return nodeInfos
}
