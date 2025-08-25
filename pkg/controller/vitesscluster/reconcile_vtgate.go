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
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	planetscalev2 "planetscale.dev/vitess-operator/pkg/apis/planetscale/v2"
	"planetscale.dev/vitess-operator/pkg/operator/reconciler"
	"planetscale.dev/vitess-operator/pkg/operator/results"
	"planetscale.dev/vitess-operator/pkg/operator/update"
	"planetscale.dev/vitess-operator/pkg/operator/vtgate"
)

func (r *ReconcileVitessCluster) reconcileVtgate(ctx context.Context, vt *planetscalev2.VitessCluster) (reconcile.Result, error) {
	// Use custom name if specified, otherwise use auto-generated name
	serviceName := vtgate.ClusterServiceName(vt.Name)
	if vt.Spec.GatewayService != nil && vt.Spec.GatewayService.Name != "" {
		serviceName = vt.Spec.GatewayService.Name
	}

	key := client.ObjectKey{Namespace: vt.Namespace, Name: serviceName}
	labels := map[string]string{
		planetscalev2.ClusterLabel:   vt.Name,
		planetscalev2.ComponentLabel: planetscalev2.VtgateComponentName,
	}
	resultBuilder := results.Builder{}

	// Clean up any old cluster-level vtgate services that don't match the current name
	if err := r.cleanupOldVtgateServices(ctx, vt, serviceName); err != nil {
		resultBuilder.Error(err)
	}

	// Reconcile vtgate Service.
	err := r.reconciler.ReconcileObject(ctx, vt, key, labels, true, reconciler.Strategy{
		Kind: &corev1.Service{},

		New: func(key client.ObjectKey) runtime.Object {
			svc := vtgate.NewService(key, labels)
			// For cluster-level service, we want to select ALL vtgate pods across all cells
			// by using only the cluster and component labels, excluding the cell label
			svc.Spec.Selector = map[string]string{
				planetscalev2.ClusterLabel:   vt.Name,
				planetscalev2.ComponentLabel: planetscalev2.VtgateComponentName,
			}
			update.ServiceOverrides(svc, vt.Spec.GatewayService)
			return svc
		},
		UpdateInPlace: func(key client.ObjectKey, obj runtime.Object) {
			svc := obj.(*corev1.Service)
			vtgate.UpdateService(svc, labels)
			// Ensure the selector is set correctly for cluster-level service
			svc.Spec.Selector = map[string]string{
				planetscalev2.ClusterLabel:   vt.Name,
				planetscalev2.ComponentLabel: planetscalev2.VtgateComponentName,
			}
			update.InPlaceServiceOverrides(svc, vt.Spec.GatewayService)
		},
		Status: func(key client.ObjectKey, obj runtime.Object) {
			svc := obj.(*corev1.Service)
			vt.Status.GatewayServiceName = svc.Name
		},
	})
	if err != nil {
		// Record error but continue.
		resultBuilder.Error(err)
	}

	return resultBuilder.Result()
}

// cleanupOldVtgateServices removes any old cluster-level vtgate services that don't match the current name
func (r *ReconcileVitessCluster) cleanupOldVtgateServices(ctx context.Context, vt *planetscalev2.VitessCluster, currentServiceName string) error {
	// List all services in the namespace with the cluster and component labels
	serviceList := &corev1.ServiceList{}
	selector := map[string]string{
		planetscalev2.ClusterLabel:   vt.Name,
		planetscalev2.ComponentLabel: planetscalev2.VtgateComponentName,
	}

	err := r.client.List(ctx, serviceList, client.InNamespace(vt.Namespace), client.MatchingLabels(selector))
	if err != nil {
		return err
	}

	// Delete any services that don't match the current name
	for _, svc := range serviceList.Items {
		// Skip cell-specific services (they have cell labels)
		if _, hasCellLabel := svc.Labels[planetscalev2.CellLabel]; hasCellLabel {
			continue
		}

		// Skip the current service
		if svc.Name == currentServiceName {
			continue
		}

		// This is an old cluster-level service, delete it
		// Note: We can't use logrus here as it's not imported, so we'll just delete silently
		if err := r.client.Delete(ctx, &svc); err != nil {
			return err
		}
	}

	return nil
}
