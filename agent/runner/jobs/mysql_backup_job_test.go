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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestXtrabackupSupportsFifo(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		output    string
		supported bool
	}{
		{"xtrabackup version 8.0.35-30 based on MySQL server 8.0.35 Linux (x86_64) (revision id: 6beb4b49)", true},
		{"xtrabackup version 8.0.33-28 based on MySQL server 8.0.33 Linux (x86_64)", true},
		{"xtrabackup version 8.4.0-6 based on MySQL server 8.4.0 Linux (x86_64)", true},
		{"xtrabackup version 8.0.32-26 based on MySQL server 8.0.32 Linux (x86_64)", false},
		{"xtrabackup version 2.4.29 based on MySQL server 5.7.44 Linux (x86_64)", false},
		{"garbage output", false},
		{"", false},
	} {
		assert.Equal(t, tc.supported, xtrabackupSupportsFifo(tc.output), "output: %q", tc.output)
	}
}
