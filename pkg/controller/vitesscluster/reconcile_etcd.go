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

package vitesscluster

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	planetscalev2 "planetscale.dev/vitess-operator/pkg/apis/planetscale/v2"
	"planetscale.dev/vitess-operator/pkg/operator/lockserver"
	"planetscale.dev/vitess-operator/pkg/operator/reconciler"
)

func (r *ReconcileVitessCluster) reconcileGlobalEtcd(ctx context.Context, vt *planetscalev2.VitessCluster) error {
	key := client.ObjectKey{
		Namespace: vt.Namespace,
		Name:      lockserver.GlobalEtcdName(vt.Name),
	}
	labels := map[string]string{
		planetscalev2.ClusterLabel:   vt.Name,
		planetscalev2.ComponentLabel: planetscalev2.EtcdComponentName,
	}
	enabled := vt.Spec.GlobalLockserver.Etcd != nil

	// Initialize status only if etcd is enabled.
	if enabled {
		vt.Status.GlobalLockserver.Etcd = planetscalev2.NewEtcdLockserverStatus()
	}

	return r.reconciler.ReconcileObject(ctx, vt, key, labels, enabled, reconciler.Strategy{
		Kind: &planetscalev2.EtcdLockserver{},

		New: func(key client.ObjectKey) runtime.Object {
			// Merge top-level cluster affinity with etcd-specific affinity
			etcdTemplate := vt.Spec.GlobalLockserver.Etcd.DeepCopy()
			if vt.Spec.Affinity != nil && etcdTemplate.Affinity != nil {
				// Merge cluster affinity with etcd affinity
				mergedAffinity := mergeClusterAndEtcdAffinity(vt.Spec.Affinity, etcdTemplate.Affinity)
				etcdTemplate.Affinity = mergedAffinity
			} else if vt.Spec.Affinity != nil {
				// Use cluster affinity if no etcd-specific affinity
				etcdTemplate.Affinity = vt.Spec.Affinity.DeepCopy()
			}
			return lockserver.NewEtcdLockserver(key, etcdTemplate, labels, "")
		},
		UpdateInPlace: func(key client.ObjectKey, obj runtime.Object) {
			newObj := obj.(*planetscalev2.EtcdLockserver)
			// Merge top-level cluster affinity with etcd-specific affinity
			etcdTemplate := vt.Spec.GlobalLockserver.Etcd.DeepCopy()
			if vt.Spec.Affinity != nil && etcdTemplate.Affinity != nil {
				// Merge cluster affinity with etcd affinity
				mergedAffinity := mergeClusterAndEtcdAffinity(vt.Spec.Affinity, etcdTemplate.Affinity)
				etcdTemplate.Affinity = mergedAffinity
			} else if vt.Spec.Affinity != nil {
				// Use cluster affinity if no etcd-specific affinity
				etcdTemplate.Affinity = vt.Spec.Affinity.DeepCopy()
			}
			lockserver.UpdateEtcdLockserver(newObj, etcdTemplate, labels, "")
		},
		Status: func(key client.ObjectKey, obj runtime.Object) {
			curObj := obj.(*planetscalev2.EtcdLockserver)
			// Make a copy of status and erase things we don't care about.
			status := curObj.Status
			status.ObservedGeneration = 0
			vt.Status.GlobalLockserver.Etcd = &status
		},
		PrepareForTurndown: func(key client.ObjectKey, obj runtime.Object) *planetscalev2.OrphanStatus {
			// Make sure it's ok to delete this etcd cluster.
			// We err on the safe side since losing etcd can be very disruptive.
			// TODO(enisoc): Define some criteria for knowing it's safe to auto-delete etcd.
			//               For now, we require manual deletion.
			return planetscalev2.NewOrphanStatus("NotSupported", "Automatic turndown is not supported for etcd for safety reasons. The EtcdLockserver instance must be deleted manually.")
		},
	})
}

// mergeClusterAndEtcdAffinity merges top-level cluster affinity with etcd-specific affinity.
// The etcd-specific affinity takes precedence for overlapping fields, but both are preserved
// to ensure the etcd pods get both the cluster-level rules and the default preferred rules.
func mergeClusterAndEtcdAffinity(clusterAffinity, etcdAffinity *corev1.Affinity) *corev1.Affinity {
	if clusterAffinity == nil {
		return etcdAffinity
	}
	if etcdAffinity == nil {
		return clusterAffinity
	}

	// Start with cluster affinity and merge etcd affinity on top
	merged := clusterAffinity.DeepCopy()

	// Merge NodeAffinity
	if etcdAffinity.NodeAffinity != nil {
		if merged.NodeAffinity == nil {
			merged.NodeAffinity = &corev1.NodeAffinity{}
		}

		// Merge RequiredDuringSchedulingIgnoredDuringExecution
		if etcdAffinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution != nil {
			if merged.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
				merged.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = &corev1.NodeSelector{}
			}
			merged.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms = append(
				merged.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms,
				etcdAffinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms...,
			)
		}

		// Merge PreferredDuringSchedulingIgnoredDuringExecution
		if etcdAffinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution != nil {
			if merged.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution == nil {
				merged.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution = []corev1.PreferredSchedulingTerm{}
			}
			merged.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution = append(
				merged.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution,
				etcdAffinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution...,
			)
		}
	}

	// Merge PodAffinity
	if etcdAffinity.PodAffinity != nil {
		if merged.PodAffinity == nil {
			merged.PodAffinity = &corev1.PodAffinity{}
		}

		// Merge RequiredDuringSchedulingIgnoredDuringExecution
		if etcdAffinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution != nil {
			if merged.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
				merged.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution = []corev1.PodAffinityTerm{}
			}
			merged.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution = append(
				merged.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution,
				etcdAffinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution...,
			)
		}

		// Merge PreferredDuringSchedulingIgnoredDuringExecution
		if etcdAffinity.PodAffinity.PreferredDuringSchedulingIgnoredDuringExecution != nil {
			if merged.PodAffinity.PreferredDuringSchedulingIgnoredDuringExecution == nil {
				merged.PodAffinity.PreferredDuringSchedulingIgnoredDuringExecution = []corev1.WeightedPodAffinityTerm{}
			}
			merged.PodAffinity.PreferredDuringSchedulingIgnoredDuringExecution = append(
				merged.PodAffinity.PreferredDuringSchedulingIgnoredDuringExecution,
				etcdAffinity.PodAffinity.PreferredDuringSchedulingIgnoredDuringExecution...,
			)
		}
	}

	// Merge PodAntiAffinity
	if etcdAffinity.PodAntiAffinity != nil {
		if merged.PodAntiAffinity == nil {
			merged.PodAntiAffinity = &corev1.PodAntiAffinity{}
		}

		// Merge RequiredDuringSchedulingIgnoredDuringExecution
		if etcdAffinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution != nil {
			if merged.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
				merged.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = []corev1.PodAffinityTerm{}
			}
			merged.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = append(
				merged.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution,
				etcdAffinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution...,
			)
		}

		// Merge PreferredDuringSchedulingIgnoredDuringExecution
		if etcdAffinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution != nil {
			if merged.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution == nil {
				merged.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution = []corev1.WeightedPodAffinityTerm{}
			}
			merged.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution = append(
				merged.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution,
				etcdAffinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution...,
			)
		}
	}

	return merged
}
