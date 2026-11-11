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

package nodeorder

import (
	"context"
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	fwk "k8s.io/kube-scheduler/framework"
	k8sframework "k8s.io/kubernetes/pkg/scheduler/framework"

	"volcano.sh/volcano/pkg/scheduler/api"
)

// scoreOnlyPlugin implements ScorePlugin and not PreScorePlugin.
type scoreOnlyPlugin struct {
	scores map[string]int64
}

func (p *scoreOnlyPlugin) Name() string { return "score-only" }

func (p *scoreOnlyPlugin) Score(_ context.Context, _ fwk.CycleState, _ *v1.Pod, nodeInfo fwk.NodeInfo) (int64, *fwk.Status) {
	return p.scores[nodeInfo.Node().Name], nil
}

func (p *scoreOnlyPlugin) ScoreExtensions() fwk.ScoreExtensions { return nil }

func TestBatchNodeOrderFnScoresPluginWithoutPreScore(t *testing.T) {
	plugin := &scoreOnlyPlugin{scores: map[string]int64{"n1": 3}}
	pp := &NodeOrderPlugin{
		ScorePlugins: map[string]scorePluginEntry{
			plugin.Name(): {plugin: plugin, weight: 2},
		},
	}
	node := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n1"}}
	nodeInfo := k8sframework.NewNodeInfo()
	nodeInfo.SetNode(node)

	scores, err := pp.BatchNodeOrderFn(
		&api.TaskInfo{Pod: &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod", Namespace: "ns"}}},
		[]*api.NodeInfo{api.NewNodeInfo(node)},
		map[string]fwk.NodeInfo{"n1": nodeInfo},
		k8sframework.NewCycleState(),
	)
	if err != nil {
		t.Fatalf("BatchNodeOrderFn returned an error: %v", err)
	}
	if scores["n1"] != 6 {
		t.Fatalf("expected weighted score 6, got %v", scores["n1"])
	}
}
