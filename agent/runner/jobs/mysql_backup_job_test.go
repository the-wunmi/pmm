// Copyright (C) 2023 Percona LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package jobs

import (
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadXtrabackupCheckpoints(t *testing.T) {
	t.Parallel()

	t.Run("full backup", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		content := "backup_type = full-backuped\nfrom_lsn = 0\nto_lsn = 2543212\nlast_lsn = 2543212\nflushed_lsn = 2543212\n"
		require.NoError(t, os.WriteFile(path.Join(dir, "xtrabackup_checkpoints"), []byte(content), 0o600))

		metadata, err := readXtrabackupCheckpoints(dir)
		require.NoError(t, err)
		assert.Equal(t, "2543212", metadata.ToLsn)
		assert.Equal(t, "0", metadata.FromLsn)
	})

	t.Run("incremental backup", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		content := "backup_type = incremental\nfrom_lsn = 2543212\nto_lsn = 2600000\nlast_lsn = 2600000\n"
		require.NoError(t, os.WriteFile(path.Join(dir, "xtrabackup_checkpoints"), []byte(content), 0o600))

		metadata, err := readXtrabackupCheckpoints(dir)
		require.NoError(t, err)
		assert.Equal(t, "2600000", metadata.ToLsn)
		assert.Equal(t, "2543212", metadata.FromLsn)
	})

	t.Run("missing file", func(t *testing.T) {
		t.Parallel()
		_, err := readXtrabackupCheckpoints(t.TempDir())
		require.Error(t, err)
	})

	t.Run("missing to_lsn", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		require.NoError(t, os.WriteFile(path.Join(dir, "xtrabackup_checkpoints"), []byte("from_lsn = 0\n"), 0o600))

		_, err := readXtrabackupCheckpoints(dir)
		require.Error(t, err)
	})
}
