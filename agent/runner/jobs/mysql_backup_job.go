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
	"bytes"
	"context"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/types/known/timestamppb"

	agentv1 "github.com/percona/pmm/api/agent/v1"
	backuppb "github.com/percona/pmm/api/backup/v1"
)

const (
	xtrabackupBin = "xtrabackup"
	xbcloudBin    = "xbcloud"
	qpressBin     = "qpress"
)

// MySQLBackupJob implements Job for MySQL backup.
type MySQLBackupJob struct {
	id             string
	timeout        time.Duration
	l              logrus.FieldLogger
	name           string
	connConf       DBConnConfig
	locationConfig BackupLocationConfig
	folder         string
	baseLSN        string
}

// NewMySQLBackupJob constructs new Job for MySQL backup.
func NewMySQLBackupJob(
	id string,
	timeout time.Duration,
	name string,
	connConf DBConnConfig,
	locationConfig BackupLocationConfig,
	folder, baseLSN string,
) *MySQLBackupJob {
	return &MySQLBackupJob{
		id:             id,
		timeout:        timeout,
		l:              logrus.WithFields(logrus.Fields{"id": id, "type": "mysql_backup", "name": name}),
		name:           name,
		connConf:       connConf,
		locationConfig: locationConfig,
		folder:         folder,
		baseLSN:        baseLSN,
	}
}

// ID returns Job id.
func (j *MySQLBackupJob) ID() string {
	return j.id
}

// Type returns Job type.
func (j *MySQLBackupJob) Type() JobType {
	return MySQLBackup
}

// Timeout returns Job timeout.
func (j *MySQLBackupJob) Timeout() time.Duration {
	return j.timeout
}

// DSN returns DSN for the Job.
func (j *MySQLBackupJob) DSN() string {
	return j.connConf.createDBURL().String()
}

// Run starts Job execution.
func (j *MySQLBackupJob) Run(ctx context.Context, send Send) error {
	err := j.binariesInstalled()
	if err != nil {
		return errors.WithStack(err)
	}

	xtrabackupMetadata, err := j.backup(ctx)
	if err != nil {
		return errors.WithStack(err)
	}

	// mysqlArtifactFiles returns list of files and folders the backup consists of (hardcoded).
	mysqlArtifactFiles := func(backupFolder string) []*backuppb.File {
		res := []*backuppb.File{
			{Name: backupFolder, IsDirectory: true},
		}
		return res
	}

	send(&agentv1.JobResult{
		JobId:     j.id,
		Timestamp: timestamppb.Now(),
		Result: &agentv1.JobResult_MysqlBackup{
			MysqlBackup: &agentv1.JobResult_MySQLBackup{
				Metadata: &backuppb.Metadata{
					FileList: mysqlArtifactFiles(j.name),
					BackupToolMetadata: &backuppb.Metadata_XtrabackupMetadata{
						XtrabackupMetadata: xtrabackupMetadata,
					},
				},
			},
		},
	})

	return nil
}

// readXtrabackupCheckpoints parses the xtrabackup_checkpoints file written via --extra-lsndir and returns the recorded LSN range.
func readXtrabackupCheckpoints(dir string) (*backuppb.XtrabackupMetadata, error) {
	content, err := os.ReadFile(path.Join(dir, "xtrabackup_checkpoints")) //nolint:gosec // path is an agent-controlled tempdir
	if err != nil {
		return nil, errors.Wrap(err, "failed to read xtrabackup_checkpoints")
	}

	metadata := &backuppb.XtrabackupMetadata{}
	for _, line := range strings.Split(string(content), "\n") {
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		switch strings.TrimSpace(key) {
		case "to_lsn":
			metadata.ToLsn = strings.TrimSpace(value)
		case "from_lsn":
			metadata.FromLsn = strings.TrimSpace(value)
		}
	}

	if metadata.ToLsn == "" {
		return nil, errors.New("to_lsn not found in xtrabackup_checkpoints")
	}

	return metadata, nil
}

func (j *MySQLBackupJob) binariesInstalled() error {
	_, err := exec.LookPath(xtrabackupBin)
	if err != nil {
		return errors.Wrapf(err, "lookpath: %s", xtrabackupBin)
	}

	_, err = exec.LookPath(qpressBin)
	if err != nil {
		return errors.Wrapf(err, "lookpath: %s", qpressBin)
	}

	if j.locationConfig.Type == S3BackupLocationType {
		_, err = exec.LookPath(xbcloudBin)
		if err != nil {
			return errors.Wrapf(err, "lookpath: %s", xbcloudBin)
		}
	}

	return nil
}

// backup streams the xtrabackup to cloud and returns the LSN range it recorded.
func (j *MySQLBackupJob) backup(ctx context.Context) (*backuppb.XtrabackupMetadata, error) {
	// --extra-lsndir writes xtrabackup_checkpoints (to_lsn/from_lsn) here even while streaming to cloud.
	lsnDir, err := os.MkdirTemp("", "mysql-backup-lsn")
	if err != nil {
		return nil, errors.Wrap(err, "failed to create LSN tempdir")
	}
	defer func() {
		if err := os.RemoveAll(lsnDir); err != nil {
			j.l.WithError(err).Warn("failed to remove LSN temporary directory")
		}
	}()

	if err := j.streamBackup(ctx, lsnDir); err != nil {
		return nil, errors.WithStack(err)
	}

	return readXtrabackupCheckpoints(lsnDir)
}

func (j *MySQLBackupJob) streamBackup(ctx context.Context, lsnDir string) (rerr error) {
	pipeCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	tmpDir, err := os.MkdirTemp("", "mysql-backup")
	if err != nil {
		return errors.Wrapf(err, "failed to create tempdir")
	}

	defer func() {
		err := os.RemoveAll(tmpDir)
		if err != nil {
			j.l.WithError(err).Warn("failed to remove temporary directory")
		}
	}()

	xtrabackupCmd := exec.CommandContext(pipeCtx,
		xtrabackupBin,
		"--compress",
		"--backup",
		// Target dir is created, even though it's empty, because we are streaming it to cloud.
		// https://jira.percona.com/browse/PXB-2602
		"--target-dir="+tmpDir,
		"--extra-lsndir="+lsnDir) // #nosec G204

	if j.baseLSN != "" {
		xtrabackupCmd.Args = append(xtrabackupCmd.Args, "--incremental-lsn="+j.baseLSN)
	}

	if j.connConf.User != "" {
		xtrabackupCmd.Args = append(xtrabackupCmd.Args, "--user="+j.connConf.User)
		xtrabackupCmd.Args = append(xtrabackupCmd.Args, "--password="+j.connConf.Password)
	}

	switch {
	case j.connConf.Address != "":
		xtrabackupCmd.Args = append(xtrabackupCmd.Args, "--host="+j.connConf.Address)
		if j.connConf.Port > 0 {
			xtrabackupCmd.Args = append(xtrabackupCmd.Args, "--port="+strconv.Itoa(j.connConf.Port))
		}
	case j.connConf.Socket != "":
		xtrabackupCmd.Args = append(xtrabackupCmd.Args, "--socket="+j.connConf.Socket)
	}

	var xbcloudCmd *exec.Cmd
	switch j.locationConfig.Type {
	case S3BackupLocationType:
		xtrabackupCmd.Args = append(xtrabackupCmd.Args, "--stream=xbstream")

		artifactFolder := path.Join(j.folder, j.name)

		j.l.Debugf("Artifact folder is: %s", artifactFolder)

		xbcloudCmd = exec.CommandContext(pipeCtx, xbcloudBin,
			"put",
			"--storage=s3",
			"--s3-endpoint="+j.locationConfig.S3Config.Endpoint,
			"--s3-access-key="+j.locationConfig.S3Config.AccessKey,
			"--s3-secret-key="+j.locationConfig.S3Config.SecretKey,
			"--s3-bucket="+j.locationConfig.S3Config.BucketName,
			"--s3-region="+j.locationConfig.S3Config.BucketRegion,
			"--parallel=10",
			artifactFolder) // #nosec G204
	default:
		return errors.Errorf("unknown location config")
	}

	var outBuffer bytes.Buffer
	var errBackupBuffer bytes.Buffer
	var errCloudBuffer bytes.Buffer
	xtrabackupCmd.Stderr = &errBackupBuffer

	xtrabackupStdout, err := xtrabackupCmd.StdoutPipe()
	if err != nil {
		return errors.Wrapf(err, "failed to get xtrabackup stdout pipe")
	}

	wrapError := func(err error) error {
		return errors.Wrapf(err, "xtrabackup err: %s\n xbcloud out: %s\n xbcloud err: %s",
			errBackupBuffer.String(), outBuffer.String(), errCloudBuffer.String())
	}

	if err := xtrabackupCmd.Start(); err != nil {
		cancel()
		return wrapError(err)
	}

	defer func() {
		err := xtrabackupCmd.Wait()
		if err != nil {
			cancel()
			if rerr != nil {
				rerr = errors.Wrapf(rerr, "xtrabackup wait error: %s", err)
			} else {
				rerr = wrapError(err)
			}
		}
	}()

	if xbcloudCmd == nil {
		return nil
	}

	xbcloudCmd.Stdin = xtrabackupStdout
	xbcloudCmd.Stdout = &outBuffer
	xbcloudCmd.Stderr = &errCloudBuffer
	if err := xbcloudCmd.Start(); err != nil {
		cancel()
		return wrapError(err)
	}

	defer func() {
		err := xbcloudCmd.Wait()
		if err != nil {
			cancel()
			if rerr != nil {
				rerr = errors.Wrapf(rerr, "xbcloud wait error: %s", err)
			} else {
				rerr = wrapError(err)
			}
		}
	}()

	return nil
}
