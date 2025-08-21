/*
Copyright 2019 PlanetScale Inc.

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

package vitessshard

import (
	"context"

	"planetscale.dev/vitess-operator/pkg/operator/update"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	planetscalev2 "planetscale.dev/vitess-operator/pkg/apis/planetscale/v2"
	"planetscale.dev/vitess-operator/pkg/operator/conditions"
	"planetscale.dev/vitess-operator/pkg/operator/reconciler"
	"planetscale.dev/vitess-operator/pkg/operator/results"
	"planetscale.dev/vitess-operator/pkg/operator/vtorc"
)

func (r *ReconcileVitessShard) reconcileVtorc(ctx context.Context, vts *planetscalev2.VitessShard) (reconcile.Result, error) {
	resultBuilder := results.Builder{}
	clusterName := vts.Labels[planetscalev2.ClusterLabel]

	labels := map[string]string{
		planetscalev2.ComponentLabel: planetscalev2.VtorcComponentName,
		planetscalev2.ClusterLabel:   clusterName,
		planetscalev2.KeyspaceLabel:  vts.Labels[planetscalev2.KeyspaceLabel],
		planetscalev2.ShardLabel:     vts.Spec.KeyRange.SafeName(),
	}

	// Reconcile vtorc Deployments.
	specs := r.vtorcSpecs(vts, labels)

	// Generate keys (object names) for all desired vtorc Deployments.
	// Keep a map back from generated names to the vtorc specs.
	keys := make([]client.ObjectKey, 0, len(specs))
	specMap := make(map[client.ObjectKey]*vtorc.Spec, len(specs))
	for _, spec := range specs {
		key := client.ObjectKey{Namespace: vts.Namespace, Name: vtorc.DeploymentName(
			clusterName,
			labels[planetscalev2.KeyspaceLabel],
			vts.Spec.KeyRange,
			spec.Cell,
		)}
		keys = append(keys, key)
		specMap[key] = spec
	}

	err := r.reconciler.ReconcileObjectSet(ctx, vts, keys, labels, reconciler.Strategy{
		Kind: &appsv1.Deployment{},

		New: func(key client.ObjectKey) runtime.Object {
			return vtorc.NewDeployment(key, specMap[key])
		},
		UpdateInPlace: func(key client.ObjectKey, obj runtime.Object) {
			newObj := obj.(*appsv1.Deployment)
			if *vts.Spec.UpdateStrategy.Type == planetscalev2.ImmediateVitessClusterUpdateStrategyType {
				vtorc.UpdateDeployment(newObj, specMap[key])
				return
			}
			vtorc.UpdateDeploymentImmediate(newObj, specMap[key])
		},
		UpdateRollingInPlace: func(key client.ObjectKey, obj runtime.Object) {
			newObj := obj.(*appsv1.Deployment)
			vtorc.UpdateDeployment(newObj, specMap[key])
		},
		Status: func(key client.ObjectKey, obj runtime.Object) {
			// This function will get called once for each Deployment.
			// Aggregate as we go to build an overall status for vtorc.
			curObj := obj.(*appsv1.Deployment)

			// We'll say orchestrator is Available overall if any of the Deployments is available.
			// The important thing is that somebody will answer when a client hits the Service.
			if available := conditions.Deployment(curObj.Status.Conditions, appsv1.DeploymentAvailable); available != nil {
				// Update the overall status if either we found one that's True, or we previously knew nothing at all (Unknown).
				if available.Status == corev1.ConditionTrue || vts.Status.VitessOrchestrator.Available == corev1.ConditionUnknown {
					vts.Status.VitessOrchestrator.Available = available.Status
				}
			}
		},
	})
	if err != nil {
		resultBuilder.Error(err)
	}

	return resultBuilder.Result()
}

func (r *ReconcileVitessShard) vtorcSpecs(vts *planetscalev2.VitessShard, parentLabels map[string]string) []*vtorc.Spec {
	if vts.Spec.VitessOrchestrator == nil {
		return nil
	}

	// Get the global cluster affinity to merge with keyspace/shard level affinity
	globalAffinity := r.getGlobalClusterAffinity(vts)

	specs := make([]*vtorc.Spec, 0, len(vts.Spec.TabletPools))

	// Deploy no more than one VTOrc per cell.
	cellMap := make(map[string]bool)

	// Make a VTOrc Deployment spec for each cell.
	for _, tabletPool := range vts.Spec.TabletPools {
		if tabletPool.Type != planetscalev2.ReplicaPoolType {
			continue
		}
		if cellMap[tabletPool.Cell] {
			continue
		}
		cellMap[tabletPool.Cell] = true

		// Copy parent labels map and add cell-specific label.
		labels := make(map[string]string, len(parentLabels)+1)
		for k, v := range parentLabels {
			labels[k] = v
		}
		labels[planetscalev2.CellLabel] = tabletPool.Cell

		// Merge ExtraVitessFlags and ExtraFlags into a new map.
		extraFlags := make(map[string]string)
		update.StringMap(&extraFlags, vts.Spec.ExtraVitessFlags)
		update.StringMap(&extraFlags, vts.Spec.VitessOrchestrator.ExtraFlags)

		// Merge global affinity with keyspace/shard level affinity
		mergedAffinity := r.mergeAffinity(globalAffinity, vts.Spec.VitessOrchestrator.Affinity)

		specs = append(specs, &vtorc.Spec{
			GlobalLockserver:  vts.Spec.GlobalLockserver,
			Image:             vts.Spec.Images.Vtorc,
			ImagePullPolicy:   vts.Spec.ImagePullPolicies.Vtorc,
			ImagePullSecrets:  vts.Spec.ImagePullSecrets,
			Keyspace:          parentLabels[planetscalev2.KeyspaceLabel],
			Shard:             vts.Spec.KeyRange.String(),
			Cell:              tabletPool.Cell,
			Zone:              vts.Spec.ZoneMap[tabletPool.Cell],
			Labels:            labels,
			Resources:         vts.Spec.VitessOrchestrator.Resources,
			Affinity:          mergedAffinity,
			ExtraFlags:        extraFlags,
			ExtraEnv:          vts.Spec.VitessOrchestrator.ExtraEnv,
			ExtraVolumes:      vts.Spec.VitessOrchestrator.ExtraVolumes,
			ExtraVolumeMounts: vts.Spec.VitessOrchestrator.ExtraVolumeMounts,
			InitContainers:    vts.Spec.VitessOrchestrator.InitContainers,
			SidecarContainers: vts.Spec.VitessOrchestrator.SidecarContainers,
			Annotations:       vts.Spec.VitessOrchestrator.Annotations,
			ExtraLabels:       vts.Spec.VitessOrchestrator.ExtraLabels,
			Tolerations:       vts.Spec.VitessOrchestrator.Tolerations,
		})
	}
	return specs
}

// getGlobalClusterAffinity retrieves the global affinity from the VitessCluster
func (r *ReconcileVitessShard) getGlobalClusterAffinity(vts *planetscalev2.VitessShard) *corev1.Affinity {
	clusterName := vts.Labels[planetscalev2.ClusterLabel]
	cluster := &planetscalev2.VitessCluster{}

	// Try to get the cluster to access global affinity
	err := r.client.Get(context.Background(), client.ObjectKey{
		Namespace: vts.Namespace,
		Name:      clusterName,
	}, cluster)

	if err != nil {
		// If we can't get the cluster, return nil (no global affinity)
		return nil
	}

	return cluster.Spec.Affinity
}

// mergeAffinity merges global affinity with local affinity, prioritizing local affinity
// Note: Zone constraints are NOT merged from global affinity as they are cell-specific
func (r *ReconcileVitessShard) mergeAffinity(global, local *corev1.Affinity) *corev1.Affinity {
	if local != nil {
		// If local affinity exists, merge global affinity into it
		if global != nil && global.NodeAffinity != nil {
			if local.NodeAffinity == nil {
				local.NodeAffinity = &corev1.NodeAffinity{}
			}
			if local.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
				local.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = &corev1.NodeSelector{}
			}

			// Ensure we have at least one term to merge into
			if len(local.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms) == 0 {
				local.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms = []corev1.NodeSelectorTerm{
					{MatchExpressions: []corev1.NodeSelectorRequirement{}},
				}
			}

			// Merge global node affinity requirements into the first local term
			// EXCEPT for zone constraints which should remain cell-specific
			for _, globalTerm := range global.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
				for _, globalExpr := range globalTerm.MatchExpressions {
					// Skip zone constraints - they should not be merged from global affinity
					if globalExpr.Key == "failure-domain.beta.kubernetes.io/zone" ||
						globalExpr.Key == "topology.kubernetes.io/zone" {
						continue
					}

					// Check if we already have this key in the first local term
					keyExists := false
					for _, localExpr := range local.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchExpressions {
						if localExpr.Key == globalExpr.Key {
							// Merge values for the same key
							for _, globalValue := range globalExpr.Values {
								found := false
								for _, existingValue := range localExpr.Values {
									if existingValue == globalValue {
										found = true
										break
									}
								}
								if !found {
									local.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchExpressions = append(
										local.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchExpressions,
										corev1.NodeSelectorRequirement{
											Key:      globalExpr.Key,
											Operator: globalExpr.Operator,
											Values:   []string{globalValue},
										},
									)
								}
							}
							keyExists = true
							break
						}
					}

					// If key doesn't exist, add it to the first local term
					if !keyExists {
						local.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchExpressions = append(
							local.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchExpressions,
							globalExpr,
						)
					}
				}
			}
		}
		return local
	}

	// If no local affinity, return global affinity (but filter out zone constraints)
	if global != nil && global.NodeAffinity != nil {
		filteredGlobal := global.DeepCopy()
		if filteredGlobal.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution != nil {
			filteredTerms := []corev1.NodeSelectorTerm{}

			for _, term := range filteredGlobal.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
				filteredMatchExpressions := []corev1.NodeSelectorRequirement{}

				for _, expr := range term.MatchExpressions {
					if expr.Key != "failure-domain.beta.kubernetes.io/zone" &&
						expr.Key != "topology.kubernetes.io/zone" {
						filteredMatchExpressions = append(filteredMatchExpressions, expr)
					}
				}

				// Only add the term if it has non-zone constraints
				if len(filteredMatchExpressions) > 0 {
					filteredTerm := term.DeepCopy()
					filteredTerm.MatchExpressions = filteredMatchExpressions
					filteredTerms = append(filteredTerms, *filteredTerm)
				}
			}

			if len(filteredTerms) > 0 {
				filteredGlobal.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms = filteredTerms
				return filteredGlobal
			}
		}
	}

	return nil
}
