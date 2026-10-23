/*
Copyright 2025 The Volcano Authors.

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
	"fmt"
	"math"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/kubernetes/pkg/features"
	kubeschedulerconfig "k8s.io/kubernetes/pkg/scheduler/apis/config"

	"volcano.sh/volcano/pkg/scheduler/framework"
)

const (
	volumeBindingTimeoutSecondsKey = "volumebinding.bindTimeoutSeconds"
	volumeBindingShapeKey          = "volumebinding.shape"
	volumeBindingWeightKey         = "volumebinding.weight"
	defaultBindTimeoutSeconds      = 600
	draFilterTimeoutSecondsKey     = "dynamicresources.filterTimeoutSeconds"
	draScoreWeightKey              = "dynamicresources.weight"
	defaultDRAFilterTimeout        = 10 * time.Second
	defaultDRAScoreWeight          = 1
)

type wrapVolumeBindingArgs struct {
	Weight int
	*kubeschedulerconfig.VolumeBindingArgs
}

type wrapDynamicResourcesArgs struct {
	// Weight scales the native DynamicResources score. Zero disables that
	// score only. Filter, Reserve and PreBind keep running.
	Weight int
	*kubeschedulerconfig.DynamicResourcesArgs
}

func defaultVolumeBindingArgs() *wrapVolumeBindingArgs {
	args := &wrapVolumeBindingArgs{
		Weight: 1,
		VolumeBindingArgs: &kubeschedulerconfig.VolumeBindingArgs{
			BindTimeoutSeconds: defaultBindTimeoutSeconds,
		},
	}

	if utilfeature.DefaultFeatureGate.Enabled(features.StorageCapacityScoring) {
		// default shape setting if VolumeCapacityPriority is enabled
		args.Shape = []kubeschedulerconfig.UtilizationShapePoint{
			{
				Utilization: 0,
				Score:       int32(kubeschedulerconfig.MaxCustomPriorityScore),
			},
			{
				Utilization: 100,
				Score:       0,
			},
		}
	}

	return args
}

// setUpVolumeBindingArgs sets up volume binding arguments from scheduler configuration.
//
// Example configuration in scheduler yaml:
//
//	actions: "enqueue, allocate, backfill"
//	tiers:
//	- plugins:
//	  - name: predicates
//	    arguments:
//	      volumebinding.bindTimeoutSeconds: 600
//	      volumebinding.weight: 1
//	      volumebinding.shape:
//	      - utilization: 0
//	        score: 10
//	      - utilization: 40
//	        score: 8
//	      - utilization: 80
//	        score: 4
//	      - utilization: 100
//	        score: 0
//		  ...
func setUpVolumeBindingArgs(vbArgs *wrapVolumeBindingArgs, rawArgs framework.Arguments) {
	if weight, ok := framework.Get[int](rawArgs, volumeBindingWeightKey); ok {
		vbArgs.Weight = weight
	}

	if timeout, ok := framework.Get[int64](rawArgs, volumeBindingTimeoutSecondsKey); ok {
		vbArgs.BindTimeoutSeconds = timeout
	}

	if utilfeature.DefaultFeatureGate.Enabled(features.StorageCapacityScoring) {
		shape, _ := framework.Get[[]kubeschedulerconfig.UtilizationShapePoint](rawArgs, volumeBindingShapeKey)
		if len(shape) != 0 {
			vbArgs.Shape = shape
		}
	}
}

func defaultDynamicResourcesArgs() *wrapDynamicResourcesArgs {
	return &wrapDynamicResourcesArgs{
		Weight: defaultDRAScoreWeight,
		DynamicResourcesArgs: &kubeschedulerconfig.DynamicResourcesArgs{
			FilterTimeout: &metav1.Duration{Duration: defaultDRAFilterTimeout},
		},
	}
}

// setUpDynamicResourcesArgs populates args from volcano framework arguments.
//
//	Supported overrides:
//	 - dynamicresources.filterTimeoutSeconds: integer seconds (e.g., 15)
//	 - dynamicresources.weight: integer >= 0 (default 1). Zero disables DRA
//	   Score only. A negative value is rejected and must not be treated as spread.
//
// Scheduler YAML decodes numbers into float64, so both int and whole-number
// float64 are accepted.
func setUpDynamicResourcesArgs(dra *wrapDynamicResourcesArgs, rawArgs framework.Arguments) error {
	weight, err := dynamicResourcesScoreWeight(rawArgs)
	if err != nil {
		return err
	}
	dra.Weight = weight
	if rawArgs == nil {
		return nil
	}

	if secs, ok := framework.Get[int](rawArgs, draFilterTimeoutSecondsKey); ok {
		if secs >= 0 {
			dra.FilterTimeout = &metav1.Duration{Duration: time.Duration(secs) * time.Second}
		}
	}
	return nil
}

func dynamicResourcesScoreWeight(rawArgs framework.Arguments) (int, error) {
	if rawArgs == nil {
		return defaultDRAScoreWeight, nil
	}
	raw, ok := rawArgs[draScoreWeightKey]
	if !ok {
		return defaultDRAScoreWeight, nil
	}
	weight, err := intArgument(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %w", draScoreWeightKey, err)
	}
	if weight < 0 {
		return 0, fmt.Errorf("invalid %s: %d, must be >= 0", draScoreWeightKey, weight)
	}
	return weight, nil
}

func intArgument(raw interface{}) (int, error) {
	switch value := raw.(type) {
	case int:
		return value, nil
	case int32:
		return int(value), nil
	case int64:
		if value > int64(math.MaxInt) || value < int64(math.MinInt) {
			return 0, fmt.Errorf("integer %d overflows", value)
		}
		return int(value), nil
	case float64:
		if math.IsNaN(value) || math.IsInf(value, 0) || value != math.Trunc(value) {
			return 0, fmt.Errorf("%v is not an integer", value)
		}
		if value > float64(math.MaxInt) || value < float64(math.MinInt) {
			return 0, fmt.Errorf("%v overflows int", value)
		}
		return int(value), nil
	default:
		return 0, fmt.Errorf("type %T is not an integer", raw)
	}
}
