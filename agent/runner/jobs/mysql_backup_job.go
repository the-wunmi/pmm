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
	"database/sql"
	"io"
	"net"
	"os"
	"os/exec"
	"path"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/hashicorp/go-version"
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

	pageTrackingUDF = "mysqlbackup_page_track_set"

	pageTrackingProbeTimeout = 30 * time.Second
)

var (
	xtrabackupVersionRegexp = regexp.MustCompile(`xtrabackup version ([!-~]*)`)
	// First XtraBackup version supporting --page-tracking.
	minPageTrackingXtrabackupVersion = version.Must(version.NewVersion("8.0.27"))
	// First XtraBackup version with the FIFO datasink.
	minFifoXtrabackupVersion = version.Must(version.NewVersion("8.0.33"))
)

func xtrabackupSupportsFifo(versionOutput string) bool {
	m := xtrabackupVersionRegexp.FindStringSubmatch(versionOutput)
	if len(m) != 2 {
		return false
	}
	v, err := version.NewVersion(m[1])
	if err != nil {
		return false
	}
	return v.Core().GreaterThanOrEqual(minFifoXtrabackupVersion)
}

// MySQLBackupJob implements Job for MySQL backup.
type MySQLBackupJob struct {
	id             string
	timeout        time.Duration
	l              logrus.FieldLogger
	name           string
	connConf       DBConnConfig
	locationConfig BackupLocationConfig
	folder         string
	compression    backuppb.BackupCompression
	baseLSN        string
}

// NewMySQLBackupJob constructs new Job for MySQL backup.
func NewMySQLBackupJob(
	id string,
	timeout time.Duration,
	name string,
	connConf DBConnConfig,
	locationConfig BackupLocationConfig,
	folder string,
	compression backuppb.BackupCompression,
	baseLSN string,
) *MySQLBackupJob {
	return &MySQLBackupJob{
		id:             id,
		timeout:        timeout,
		l:              logrus.WithFields(logrus.Fields{"id": id, "type": "mysql_backup", "name": name}),
		name:           name,
		connConf:       connConf,
		locationConfig: locationConfig,
		folder:         folder,
		compression:    compression,
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

func xtrabackupSupportsPageTracking(versionOutput string) bool {
	m := xtrabackupVersionRegexp.FindStringSubmatch(versionOutput)
	if len(m) != 2 {
		return false
	}
	v, err := version.NewVersion(m[1])
	if err != nil {
		return false
	}
	return v.Core().GreaterThanOrEqual(minPageTrackingXtrabackupVersion)
}

func (j *MySQLBackupJob) openMySQL() (*sql.DB, error) {
	cfg := mysql.NewConfig()
	cfg.User = j.connConf.User
	cfg.Passwd = j.connConf.Password
	cfg.TLSConfig = "preferred"
	if j.connConf.Address != "" {
		cfg.Net = "tcp"
		cfg.Addr = net.JoinHostPort(j.connConf.Address, strconv.Itoa(j.connConf.Port))
	} else {
		cfg.Net = "unix"
		cfg.Addr = j.connConf.Socket
	}

	connector, err := mysql.NewConnector(cfg)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return sql.OpenDB(connector), nil
}

func (j *MySQLBackupJob) pageTrackingUsable(ctx context.Context) bool {
	probeCtx, cancel := context.WithTimeout(ctx, pageTrackingProbeTimeout)
	defer cancel()

	out, err := exec.CommandContext(probeCtx, xtrabackupBin, "--version").CombinedOutput()
	if err != nil {
		j.l.WithError(err).Debug("failed to get xtrabackup version")
		return false
	}
	if !xtrabackupSupportsPageTracking(string(out)) {
		return false
	}

	db, err := j.openMySQL()
	if err != nil {
		j.l.WithError(err).Debug("failed to open mysql connection for page tracking probe")
		return false
	}
	defer db.Close() //nolint:errcheck

	var one int
	err = db.QueryRowContext(probeCtx,
		"SELECT 1 FROM performance_schema.user_defined_functions WHERE udf_name = ? LIMIT 1",
		pageTrackingUDF).Scan(&one)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return false
	case err != nil:
		j.l.WithError(err).Debug("failed to check mysqlbackup component")
		return false
	}
	return true
}

func (j *MySQLBackupJob) binariesInstalled() error {
	_, err := exec.LookPath(xtrabackupBin)
	if err != nil {
		return errors.Wrapf(err, "lookpath: %s", xtrabackupBin)
	}

	if j.compression == backuppb.BackupCompression_BACKUP_COMPRESSION_QUICKLZ {
		if _, err := exec.LookPath(qpressBin); err != nil {
			return errors.Wrapf(err, "lookpath: %s", qpressBin)
		}
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

	fifoStreams := 0
	if j.locationConfig.Type == S3BackupLocationType {
		if out, err := exec.CommandContext(pipeCtx, xtrabackupBin, "--version").CombinedOutput(); err == nil &&
			xtrabackupSupportsFifo(string(out)) {
			fifoStreams = 10
		}
	}

	xtrabackupCmd := exec.CommandContext(pipeCtx,
		xtrabackupBin,
		"--backup",
		// Target dir is created, even though it's empty, because we are streaming it to cloud.
		// https://jira.percona.com/browse/PXB-2602
		"--target-dir="+tmpDir,
		"--extra-lsndir="+lsnDir) // #nosec G204

	// Copy threads feed the FIFO streams when the datasink is active; compression threads stay CPU-bound.
	threads := strconv.Itoa(max(2, min(8, runtime.NumCPU()/2)))
	parallel := threads
	if fifoStreams > 0 {
		parallel = strconv.Itoa(fifoStreams)
	}
	xtrabackupCmd.Args = append(xtrabackupCmd.Args, "--parallel="+parallel, "--compress-threads="+threads)

	if j.baseLSN != "" {
		xtrabackupCmd.Args = append(xtrabackupCmd.Args, "--incremental-lsn="+j.baseLSN)
	}

	switch j.compression {
	case backuppb.BackupCompression_BACKUP_COMPRESSION_DEFAULT:
		xtrabackupCmd.Args = append(xtrabackupCmd.Args, "--compress")
	case backuppb.BackupCompression_BACKUP_COMPRESSION_QUICKLZ:
		xtrabackupCmd.Args = append(xtrabackupCmd.Args, "--compress=quicklz")
	case backuppb.BackupCompression_BACKUP_COMPRESSION_ZSTD:
		xtrabackupCmd.Args = append(xtrabackupCmd.Args, "--compress=zstd")
	case backuppb.BackupCompression_BACKUP_COMPRESSION_LZ4:
		xtrabackupCmd.Args = append(xtrabackupCmd.Args, "--compress=lz4")
	case backuppb.BackupCompression_BACKUP_COMPRESSION_NONE:
	default:
		return errors.Errorf("unknown compression: %s", j.compression)
	}

	if j.pageTrackingUsable(ctx) {
		xtrabackupCmd.Args = append(xtrabackupCmd.Args, "--page-tracking")
	}

	var fifoDir string
	if fifoStreams > 0 {
		fifoDir, err = os.MkdirTemp("", "mysql-backup-fifo")
		if err != nil {
			return errors.Wrap(err, "failed to create FIFO tempdir")
		}
		defer func() {
			if err := os.RemoveAll(fifoDir); err != nil {
				j.l.WithError(err).Warn("failed to remove FIFO temporary directory")
			}
		}()

		j.l.Infof("Using FIFO datasink with %d streams.", fifoStreams)
		xtrabackupCmd.Args = append(xtrabackupCmd.Args,
			"--fifo-streams="+strconv.Itoa(fifoStreams),
			"--fifo-dir="+fifoDir)
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

		if fifoStreams > 0 {
			xbcloudCmd.Args = append(xbcloudCmd.Args,
				"--fifo-streams="+strconv.Itoa(fifoStreams),
				"--fifo-dir="+fifoDir)
		}
	default:
		return errors.Errorf("unknown location config")
	}

	var outBuffer bytes.Buffer
	var errBackupBuffer bytes.Buffer
	var errCloudBuffer bytes.Buffer
	xtrabackupCmd.Stderr = &errBackupBuffer

	var xtrabackupStdout io.ReadCloser
	if fifoStreams == 0 {
		xtrabackupStdout, err = xtrabackupCmd.StdoutPipe()
		if err != nil {
			return errors.Wrapf(err, "failed to get xtrabackup stdout pipe")
		}
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

	if fifoStreams == 0 {
		xbcloudCmd.Stdin = xtrabackupStdout
	}
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
