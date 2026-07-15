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

package agents

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"

	backuppb "github.com/percona/pmm/api/backup/v1"
	"github.com/percona/pmm/managed/models"
)

func TestArtifactMetadataFromProto(t *testing.T) {
	t.Run("all fields are filled", func(t *testing.T) {
		protoMetadata := backuppb.Metadata{
			FileList:           []*backuppb.File{{Name: "dir1", IsDirectory: true}, {Name: "file1"}, {Name: "file2"}},
			RestoreTo:          &timestamppb.Timestamp{Seconds: 123, Nanos: 456},
			BackupToolMetadata: &backuppb.Metadata_PbmMetadata{PbmMetadata: &backuppb.PbmMetadata{Name: "some name"}},
		}

		expected := &models.Metadata{
			FileList:       []models.File{{Name: "dir1", IsDirectory: true}, {Name: "file1"}, {Name: "file2"}},
			RestoreTo:      new(time.Unix(123, 456).UTC()),
			BackupToolData: &models.BackupToolData{PbmMetadata: &models.PbmMetadata{Name: "some name"}},
		}

		actual := artifactMetadataFromProto(&protoMetadata)
		assert.Equal(t, expected, actual)
	})

	t.Run("some fields are empty", func(t *testing.T) {
		protoMetadata := backuppb.Metadata{
			FileList: []*backuppb.File{{Name: "dir1", IsDirectory: true}, {Name: "file1"}, {Name: "file2"}},
		}

		expected := &models.Metadata{
			FileList: []models.File{{Name: "dir1", IsDirectory: true}, {Name: "file1"}, {Name: "file2"}},
		}

		actual := artifactMetadataFromProto(&protoMetadata)
		assert.Equal(t, expected, actual)
	})

	t.Run("argument is nil", func(t *testing.T) {
		var protoMetadata *backuppb.Metadata
		var expected *models.Metadata

		actual := artifactMetadataFromProto(protoMetadata)
		assert.Equal(t, expected, actual)
	})
}

func TestIncrementalMetadataMatchesBase(t *testing.T) {
	t.Parallel()

	xtrabackup := func(fromLSN string) *models.Metadata {
		return &models.Metadata{BackupToolData: &models.BackupToolData{
			XtrabackupMetadata: &models.XtrabackupMetadata{FromLSN: fromLSN, ToLSN: "9999"},
		}}
	}

	assert.True(t, incrementalMetadataMatchesBase(xtrabackup("2543212"), "2543212"))
	// Agent ran a full backup (from_lsn 0) instead of an incremental against the base.
	assert.False(t, incrementalMetadataMatchesBase(xtrabackup("0"), "2543212"))
	// Agent chained off the wrong base.
	assert.False(t, incrementalMetadataMatchesBase(xtrabackup("111"), "2543212"))
	// Old agent returned no xtrabackup metadata at all.
	assert.False(t, incrementalMetadataMatchesBase(&models.Metadata{}, "2543212"))
	assert.False(t, incrementalMetadataMatchesBase(nil, "2543212"))
}
