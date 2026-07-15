// Copyright (C) 2023 Percona LLC
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <https://www.gnu.org/licenses/>.

package backup

import (
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"gopkg.in/reform.v1"

	"github.com/percona/pmm/managed/models"
)

// RetentionService handles retention for artifacts.
type RetentionService struct {
	db         *reform.DB
	l          *logrus.Entry
	removalSVC removalService
}

// NewRetentionService creates new retention service for artifacts.
func NewRetentionService(db *reform.DB, removalSVC removalService) *RetentionService {
	return &RetentionService{
		l:          logrus.WithField("component", "management/backup/retention"),
		db:         db,
		removalSVC: removalSVC,
	}
}

// EnforceRetention enforce retention on provided scheduled backup task
// it removes any old successful artifacts below retention threshold.
func (s *RetentionService) EnforceRetention(scheduleID string) error {
	task, err := models.FindScheduledTaskByID(s.db.Querier, scheduleID)
	if err != nil {
		return err
	}

	retention, err := task.Retention()
	if err != nil {
		return err
	}

	if retention == 0 {
		return nil
	}

	mode, err := task.Mode()
	if err != nil {
		return err
	}

	locationID, err := task.LocationID()
	if err != nil {
		return err
	}

	location, err := models.FindBackupLocationByID(s.db.Querier, locationID)
	if err != nil {
		return err
	}

	storage := GetStorageForLocation(location)

	switch mode {
	case models.Snapshot, models.Incremental:
		err = s.retentionByCount(storage, scheduleID, retention)
	case models.PITR:
		err = s.retentionPITR(storage, scheduleID, retention)
	default:
		s.l.Warnf("Retention policy is not implemented for backup mode %s", mode)
		return nil
	}

	return err
}

// retentionByCount keeps the newest `retention` successful artifacts of a schedule and deletes the
// rest, but never a base while an incremental chained off it survives (see deletableArtifacts).
func (s *RetentionService) retentionByCount(storage Storage, scheduleID string, retention uint32) error {
	artifacts, err := models.FindArtifacts(s.db.Querier, models.ArtifactFilters{
		ScheduleID: scheduleID,
		Status:     models.SuccessBackupStatus,
	})
	if err != nil {
		return err
	}

	if int(retention) >= len(artifacts) {
		return nil
	}

	// Spans every schedule, so a child in another schedule still pins its base.
	allArtifacts, err := models.FindArtifacts(s.db.Querier, models.ArtifactFilters{})
	if err != nil {
		return err
	}

	for _, artifact := range deletableArtifacts(artifacts[retention:], allArtifacts) {
		err := s.removalSVC.DeleteArtifact(storage, artifact.ID, true)
		switch {
		case err == nil:
		case errors.Is(err, ErrArtifactHasChildren):
			// A child's async deletion hasn't finished; defer to a later retention run.
			s.l.Debugf("Deferring deletion of artifact %q: %v", artifact.ID, err)
		default:
			return err
		}
	}

	return nil
}

// deletableArtifacts returns the candidates safe to delete: a candidate is excluded if any artifact
// chained off it (transitively) is not itself a candidate, so allArtifacts must span all schedules.
// The candidates order (newest-first) is preserved, which is children-before-parents within a chain.
func deletableArtifacts(candidates, allArtifacts []*models.Artifact) []*models.Artifact {
	candidateSet := make(map[string]struct{}, len(candidates))
	for _, artifact := range candidates {
		candidateSet[artifact.ID] = struct{}{}
	}

	childrenOf := make(map[string][]*models.Artifact)
	for _, artifact := range allArtifacts {
		if artifact.ParentArtifactID != nil {
			parentID := *artifact.ParentArtifactID
			childrenOf[parentID] = append(childrenOf[parentID], artifact)
		}
	}

	memo := make(map[string]bool)
	var hasSurvivingDescendant func(id string) bool
	hasSurvivingDescendant = func(id string) bool {
		if v, ok := memo[id]; ok {
			return v
		}
		// Guard against cycles: assume no surviving descendant while recursing.
		memo[id] = false
		res := false
		for _, child := range childrenOf[id] {
			// A kept child (not a candidate), or a kept descendant below it.
			if _, ok := candidateSet[child.ID]; !ok || hasSurvivingDescendant(child.ID) {
				res = true
				break
			}
		}
		memo[id] = res
		return res
	}

	deletable := make([]*models.Artifact, 0, len(candidates))
	for _, artifact := range candidates {
		if !hasSurvivingDescendant(artifact.ID) {
			deletable = append(deletable, artifact)
		}
	}
	return deletable
}

func (s *RetentionService) retentionPITR(storage Storage, scheduleID string, retention uint32) error {
	artifacts, err := models.FindArtifacts(s.db.Querier, models.ArtifactFilters{
		ScheduleID: scheduleID,
		Status:     models.SuccessBackupStatus,
	})
	if err != nil {
		return err
	}

	if len(artifacts) == 0 {
		return nil
	}

	if len(artifacts) > 1 {
		return errors.Errorf("Can be only one artifact entity for PITR in the database but found %d", len(artifacts))
	}

	artifact := artifacts[0]
	trimBy := len(artifact.MetadataList) - int(retention)
	if trimBy <= 0 {
		return nil
	}

	return s.removalSVC.TrimPITRArtifact(storage, artifact.ID, trimBy)
}
