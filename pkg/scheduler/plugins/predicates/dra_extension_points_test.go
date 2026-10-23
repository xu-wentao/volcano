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
	"reflect"
	"testing"

	k8sframework "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/dynamicresources"
)

// draExtensionPointDecisions classifies every exported method of
// dynamicresources.DynamicResources for the Kubernetes version in go.mod.
// A dependency upgrade that adds or removes a method fails this test until the
// new extension point is explicitly adapted or ignored.
var draExtensionPointDecisions = map[string]string{
	"Name":                "adapted: plugin id",
	"PreFilter":           "adapted: predicates PrePredicate",
	"PreFilterExtensions": "adapted: returns nil, so there is no AddPod or RemovePod",
	"Filter":              "adapted: predicates Predicate",
	"Score":               "adapted: predicates BatchNodeOrder, same CycleState as Filter",
	"ScoreExtensions":     "adapted: nodescore.CalculatePluginScore reads the plugin normalizer",
	"NormalizeScore":      "adapted: nodescore.CalculatePluginScore",
	"Reserve":             "adapted: allocate event",
	"Unreserve":           "adapted: deallocate and PreBind rollback",
	"PreBind":             "adapted: predicates PreBind",
	"PreBindPreFlight":    "ignored: volcano decides whether to run PreBind with needsPreBind",
	"PreEnqueue":          "ignored: volcano does not use the kube-scheduler scheduling queue",
	"EventsToRegister":    "ignored: queueing hints belong to the kube-scheduler queue",
	"PostFilter":          "ignored: kube-scheduler preemption post-filter; volcano preempt and reclaim do not call it",
	"SignPod":             "ignored: volcano does not use kube-scheduler pod signatures",
}

func TestDynamicResourcesExtensionPointDecisions(t *testing.T) {
	plugin := &dynamicresources.DynamicResources{}
	methodType := reflect.TypeOf(plugin)
	implemented := map[string]struct{}{}
	for i := 0; i < methodType.NumMethod(); i++ {
		name := methodType.Method(i).Name
		implemented[name] = struct{}{}
		if _, ok := draExtensionPointDecisions[name]; !ok {
			t.Errorf("DynamicResources implements %s, but Volcano has not marked it as adapted or intentionally ignored", name)
		}
	}
	for name := range draExtensionPointDecisions {
		if _, ok := implemented[name]; !ok {
			t.Errorf("decision list contains %s, but DynamicResources no longer implements it", name)
		}
	}

	if implementsInterface[k8sframework.PreScorePlugin](plugin) {
		t.Error("DynamicResources now implements PreScorePlugin; register it in PreScoreOrder and cover it in the decision list")
	}
	assertImplements[k8sframework.PreFilterPlugin](t, plugin)
	assertImplements[k8sframework.FilterPlugin](t, plugin)
	assertImplements[k8sframework.PostFilterPlugin](t, plugin)
	assertImplements[k8sframework.ScorePlugin](t, plugin)
	assertImplements[k8sframework.ReservePlugin](t, plugin)
	assertImplements[k8sframework.PreBindPlugin](t, plugin)
	assertImplements[k8sframework.PreEnqueuePlugin](t, plugin)
	assertImplements[k8sframework.EnqueueExtensions](t, plugin)
	assertImplements[k8sframework.SignPlugin](t, plugin)
	if plugin.ScoreExtensions() == nil {
		t.Fatal("DynamicResources.ScoreExtensions() is nil; NormalizeScore would be skipped")
	}
}

func implementsInterface[T any](plugin any) bool {
	_, ok := plugin.(T)
	return ok
}

func assertImplements[T any](t *testing.T, plugin any) {
	t.Helper()
	if !implementsInterface[T](plugin) {
		var zero T
		t.Errorf("DynamicResources no longer implements %T", zero)
	}
}
