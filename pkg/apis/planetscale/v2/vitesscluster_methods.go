/*
Copyright 2020 PlanetScale Inc.

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

package v2

import (
	corev1 "k8s.io/api/core/v1"
)

// Cell looks up an item in the Cells list by name.
// It returns a pointer to the item, or nil if the specified cell doesn't exist.
func (s *VitessClusterSpec) Cell(cellName string) *VitessCellTemplate {
	for i := range s.Cells {
		if s.Cells[i].Name == cellName {
			return &s.Cells[i]
		}
	}
	return nil
}

// ZoneMap returns a map from cell names to zone names.
func (s *VitessClusterSpec) ZoneMap() map[string]string {
	zones := make(map[string]string, len(s.Cells))
	for i := range s.Cells {
		cell := &s.Cells[i]
		zones[cell.Name] = cell.Zone
	}
	return zones
}

// AffinityMap returns a map from cell names to their affinity settings.
// Top-level cluster affinity is merged with cell-specific affinity settings.
func (s *VitessClusterSpec) AffinityMap() map[string]*corev1.Affinity {
	affinities := make(map[string]*corev1.Affinity, len(s.Cells))
	for i := range s.Cells {
		cell := &s.Cells[i]

		// Start with top-level cluster affinity (if any)
		var mergedAffinity *corev1.Affinity
		if s.Affinity != nil {
			// Deep copy the top-level affinity to avoid modifying the original
			mergedAffinity = s.Affinity.DeepCopy()
		}

		// Merge with cell-specific affinity if it exists
		if cell.Affinity != nil {
			if mergedAffinity == nil {
				// No top-level affinity, just use cell affinity
				mergedAffinity = cell.Affinity.DeepCopy()
			} else {
				// Merge top-level and cell affinity
				mergedAffinity = mergeAffinities(mergedAffinity, cell.Affinity)
			}
		}

		if mergedAffinity != nil {
			affinities[cell.Name] = mergedAffinity
		}
	}
	return affinities
}

// mergeAffinities merges two affinity objects, with the second one taking precedence
// for fields that are set in both. Zone constraints are NOT merged from global affinity
// to maintain cell isolation.
func mergeAffinities(base, override *corev1.Affinity) *corev1.Affinity {
	if base == nil {
		return override.DeepCopy()
	}
	if override == nil {
		return base.DeepCopy()
	}

	merged := base.DeepCopy()

	// Merge NodeAffinity
	if override.NodeAffinity != nil {
		if merged.NodeAffinity == nil {
			merged.NodeAffinity = &corev1.NodeAffinity{}
		}

		// Merge RequiredDuringSchedulingIgnoredDuringExecution
		if override.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution != nil {
			if merged.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
				merged.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = &corev1.NodeSelector{}
			}

			// Filter out zone constraints from base affinity to prevent merging
			filteredBaseTerms := []corev1.NodeSelectorTerm{}
			for _, term := range merged.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
				filteredMatchExpressions := []corev1.NodeSelectorRequirement{}
				for _, expr := range term.MatchExpressions {
					// Skip zone constraints from base affinity
					if expr.Key != "failure-domain.beta.kubernetes.io/zone" &&
						expr.Key != "topology.kubernetes.io/zone" {
						filteredMatchExpressions = append(filteredMatchExpressions, expr)
					}
				}
				if len(filteredMatchExpressions) > 0 {
					filteredTerm := term.DeepCopy()
					filteredTerm.MatchExpressions = filteredMatchExpressions
					filteredBaseTerms = append(filteredBaseTerms, *filteredTerm)
				}
			}

			// Start with filtered base terms
			merged.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms = filteredBaseTerms

			// Add override terms (which include cell-specific zone constraints)
			merged.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms = append(
				merged.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms,
				override.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms...,
			)
		}

		// Merge PreferredDuringSchedulingIgnoredDuringExecution
		if override.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution != nil {
			if merged.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution == nil {
				merged.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution = []corev1.PreferredSchedulingTerm{}
			}
			merged.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution = append(
				merged.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution,
				override.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution...,
			)
		}
	}

	// Merge PodAffinity
	if override.PodAffinity != nil {
		if merged.PodAffinity == nil {
			merged.PodAffinity = &corev1.PodAffinity{}
		}

		// Merge RequiredDuringSchedulingIgnoredDuringExecution
		if override.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution != nil {
			if merged.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
				merged.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution = []corev1.PodAffinityTerm{}
			}
			merged.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution = append(
				merged.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution,
				override.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution...,
			)
		}

		// Merge PreferredDuringSchedulingIgnoredDuringExecution
		if override.PodAffinity.PreferredDuringSchedulingIgnoredDuringExecution != nil {
			if merged.PodAffinity.PreferredDuringSchedulingIgnoredDuringExecution == nil {
				merged.PodAffinity.PreferredDuringSchedulingIgnoredDuringExecution = []corev1.WeightedPodAffinityTerm{}
			}
			merged.PodAffinity.PreferredDuringSchedulingIgnoredDuringExecution = append(
				merged.PodAffinity.PreferredDuringSchedulingIgnoredDuringExecution,
				override.PodAffinity.PreferredDuringSchedulingIgnoredDuringExecution...,
			)
		}
	}

	// Merge PodAntiAffinity
	if override.PodAntiAffinity != nil {
		if merged.PodAntiAffinity == nil {
			merged.PodAntiAffinity = &corev1.PodAntiAffinity{}
		}

		// Merge RequiredDuringSchedulingIgnoredDuringExecution
		if override.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution != nil {
			if merged.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
				merged.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = []corev1.PodAffinityTerm{}
			}
			merged.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = append(
				merged.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution,
				override.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution...,
			)
		}

		// Merge PreferredDuringSchedulingIgnoredDuringExecution
		if override.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution != nil {
			if merged.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution == nil {
				merged.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution = []corev1.WeightedPodAffinityTerm{}
			}
			merged.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution = append(
				merged.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution,
				override.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution...,
			)
		}
	}

	return merged
}

// Image returns the first mysqld flavor image that's set.
func (image *MysqldImage) Image() string {
	switch {
	case image.Mysql56Compatible != "":
		return image.Mysql56Compatible
	case image.Mysql80Compatible != "":
		return image.Mysql80Compatible
	case image.MariadbCompatible != "":
		return image.MariadbCompatible
	case image.Mariadb103Compatible != "":
		return image.Mariadb103Compatible
	default:
		return ""
	}
}

// Flavor returns Vitess flavor setting value
// for the first flavor that has an image set.
func (image *MysqldImage) Flavor() string {
	switch {
	case image.Mysql56Compatible != "":
		return "MySQL56"
	case image.Mysql80Compatible != "":
		return "MySQL80"
	case image.MariadbCompatible != "":
		return "MariaDB"
	case image.Mariadb103Compatible != "":
		return "MariaDB103"
	default:
		return ""
	}
}

func (externalOptions *ExternalVitessClusterUpdateStrategyOptions) ResourceChangesAllowed(resource corev1.ResourceName) bool {
	for _, resourceOption := range externalOptions.AllowResourceChanges {
		if resourceOption == resource {
			return true
		}
	}

	return false
}
