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
	"io"
	"os"
	"os/exec"
	"os/user"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/types/known/timestamppb"

	agentv1 "github.com/percona/pmm/api/agent/v1"
	backupv1 "github.com/percona/pmm/api/backup/v1"
)

const (
	xbstreamBin          = "xbstream"
	mySQLSystemUserName  = "mysql"
	mySQLSystemGroupName = "mysql"
	// TODO make mySQLDirectory autorecognized as done in 'xtrabackup' utility; see 'xtrabackup --help' --datadir parameter.
	mySQLDirectory   = "/var/lib/mysql"
	systemctlTimeout = 10 * time.Second
	// Thread count for the download, decompress and prepare phases.
	restoreParallelism = "10"
)

var mysqlServiceRegex = regexp.MustCompile(`mysql(d)?\.service`) // this is used to lookup MySQL service in the list of all system services

// MySQLRestoreJob implements Job for MySQL backup restore.
type MySQLRestoreJob struct {
	id             string
	timeout        time.Duration
	l              logrus.FieldLogger
	name           string
	baseNames      []string
	locationConfig BackupLocationConfig
	folder         string
	compression    backupv1.BackupCompression
}

// NewMySQLRestoreJob constructs new Job for MySQL backup restore.
func NewMySQLRestoreJob(
	id string,
	timeout time.Duration,
	name string,
	baseNames []string,
	locationConfig BackupLocationConfig,
	folder string,
	compression backupv1.BackupCompression,
) *MySQLRestoreJob {
	return &MySQLRestoreJob{
		id:             id,
		timeout:        timeout,
		l:              logrus.WithFields(logrus.Fields{"id": id, "type": "mysql_restore"}),
		name:           name,
		baseNames:      baseNames,
		locationConfig: locationConfig,
		folder:         folder,
		compression:    compression,
	}
}

// ID returns job id.
func (j *MySQLRestoreJob) ID() string {
	return j.id
}

// Type returns job type.
func (j *MySQLRestoreJob) Type() JobType {
	return MySQLRestore
}

// Timeout returns job timeout.
func (j *MySQLRestoreJob) Timeout() time.Duration {
	return j.timeout
}

// DSN returns DSN for the Job.
func (j *MySQLRestoreJob) DSN() string {
	return "" // not used for MySQL restore
}

// Run executes backup restore steps.
func (j *MySQLRestoreJob) Run(ctx context.Context, send Send) error {
	if j.locationConfig.S3Config == nil {
		return errors.New("S3 config is not set")
	}

	if err := j.binariesInstalled(); err != nil {
		return errors.WithStack(err)
	}

	if _, _, err := mySQLUserAndGroupIDs(); err != nil {
		return errors.WithStack(err)
	}

	tmpDir, err := os.MkdirTemp("", "backup-restore")
	if err != nil {
		return errors.Wrap(err, "cannot create temporary directory")
	}
	defer func() {
		err := os.RemoveAll(tmpDir)
		if err != nil {
			j.l.WithError(err).Warn("failed to remove temporary directory")
		}
	}()

	mySQLServiceName, err := getMysqlServiceName(ctx)
	if err != nil {
		return errors.WithStack(err)
	}
	j.l.Debugf("Using MySQL service name: %s", mySQLServiceName)

	preparedDir, err := j.prepareRestoreChain(ctx, tmpDir)
	if err != nil {
		return errors.WithStack(err)
	}

	active, err := mySQLActive(ctx, mySQLServiceName)
	if err != nil {
		return errors.WithStack(err)
	}
	if active {
		err := stopMySQL(ctx, mySQLServiceName)
		if err != nil {
			return errors.WithStack(err)
		}
	}

	if err := restoreBackup(ctx, preparedDir, mySQLDirectory); err != nil {
		return errors.WithStack(err)
	}

	if err := startMySQL(ctx, mySQLServiceName); err != nil {
		return errors.WithStack(err)
	}

	send(&agentv1.JobResult{
		JobId:     j.id,
		Timestamp: timestamppb.Now(),
		Result: &agentv1.JobResult_MysqlRestoreBackup{
			MysqlRestoreBackup: &agentv1.JobResult_MySQLRestoreBackup{},
		},
	})

	return nil
}

func (j *MySQLRestoreJob) binariesInstalled() error {
	_, err := exec.LookPath(xtrabackupBin)
	if err != nil {
		return errors.Wrapf(err, "lookpath: %s", xtrabackupBin)
	}

	_, err = exec.LookPath(xbcloudBin)
	if err != nil {
		return errors.Wrapf(err, "lookpath: %s", xbcloudBin)
	}

	_, err = exec.LookPath(xbstreamBin)
	if err != nil {
		return errors.Wrapf(err, "lookpath: %s", xbstreamBin)
	}

	if j.compression == backupv1.BackupCompression_BACKUP_COMPRESSION_QUICKLZ {
		if _, err := exec.LookPath(qpressBin); err != nil {
			return errors.Wrapf(err, "lookpath: %s", qpressBin)
		}
	}

	return nil
}

func prepareRestoreCommands( //nolint:nonamedreturns
	ctx context.Context,
	folder string,
	config *BackupLocationConfig,
	targetDirectory string,
	fifoDir string,
	fifoStreams int,
	stderr io.Writer,
	stdout io.Writer,
) (xbcloud, xbstream *exec.Cmd, _ error) {
	xbcloudCmd := exec.CommandContext( //nolint:gosec
		ctx,
		xbcloudBin,
		"get",
		"--storage=s3",
		"--s3-endpoint="+config.S3Config.Endpoint,
		"--s3-access-key="+config.S3Config.AccessKey,
		"--s3-secret-key="+config.S3Config.SecretKey,
		"--s3-bucket="+config.S3Config.BucketName,
		"--s3-region="+config.S3Config.BucketRegion,
		"--parallel="+restoreParallelism,
		folder,
	)
	xbcloudCmd.Stderr = stderr

	xbstreamCmd := exec.CommandContext( //nolint:gosec
		ctx,
		xbstreamBin,
		"restore",
		"-x",
		"--directory="+targetDirectory,
		"--parallel="+restoreParallelism,
	)
	xbstreamCmd.Stderr = stderr
	xbstreamCmd.Stdout = stdout

	if fifoStreams > 0 {
		fifoArgs := []string{
			"--fifo-streams=" + strconv.Itoa(fifoStreams),
			"--fifo-dir=" + fifoDir,
		}
		xbcloudCmd.Args = append(xbcloudCmd.Args, fifoArgs...)
		xbstreamCmd.Args = append(xbstreamCmd.Args, fifoArgs...)
		return xbcloudCmd, xbstreamCmd, nil
	}

	xbcloudStdout, err := xbcloudCmd.StdoutPipe()
	if err != nil {
		return nil, nil, errors.Wrapf(err, "failed to get xbcloud stdout pipe")
	}
	xbstreamCmd.Stdin = xbcloudStdout

	return xbcloudCmd, xbstreamCmd, nil
}

func (j *MySQLRestoreJob) prepareRestoreChain(ctx context.Context, workDir string) (string, error) {
	chain := make([]string, 0, len(j.baseNames)+1)
	chain = append(chain, j.baseNames...)
	chain = append(chain, j.name)

	baseDirectory := filepath.Join(workDir, "base")
	if err := os.MkdirAll(baseDirectory, 0o750); err != nil {
		return "", errors.Wrap(err, "failed to create base restore directory")
	}

	for i, backupName := range chain {
		applyLogOnly := i < len(chain)-1

		if i == 0 {
			if err := j.downloadAndExtract(ctx, backupName, baseDirectory); err != nil {
				return "", errors.Wrapf(err, "failed to download base backup %q", backupName)
			}
			if err := decompressBackup(ctx, baseDirectory, j.compression); err != nil {
				return "", err
			}
			if err := prepareBackup(ctx, baseDirectory, "", applyLogOnly); err != nil {
				return "", err
			}
			continue
		}

		incrementalDirectory := filepath.Join(workDir, "increment-"+strconv.Itoa(i))
		if err := os.MkdirAll(incrementalDirectory, 0o750); err != nil {
			return "", errors.Wrapf(err, "failed to create increment directory for %q", backupName)
		}
		if err := j.downloadAndExtract(ctx, backupName, incrementalDirectory); err != nil {
			return "", errors.Wrapf(err, "failed to download increment %q", backupName)
		}
		if err := decompressBackup(ctx, incrementalDirectory, j.compression); err != nil {
			return "", err
		}
		if err := prepareBackup(ctx, baseDirectory, incrementalDirectory, applyLogOnly); err != nil {
			return "", err
		}
		if err := os.RemoveAll(incrementalDirectory); err != nil {
			j.l.WithError(err).Warn("failed to remove merged increment directory")
		}
	}

	return baseDirectory, nil
}

func (j *MySQLRestoreJob) downloadAndExtract(ctx context.Context, backupName, targetDirectory string) (rerr error) {
	pipeCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var stderr, stdout bytes.Buffer

	artifactFolder := path.Join(j.folder, backupName)

	j.l.Debugf("Artifact folder is: %s", artifactFolder)

	fifoStreams := 0
	if out, err := exec.CommandContext(pipeCtx, xtrabackupBin, "--version").CombinedOutput(); err == nil &&
		xtrabackupSupportsFifo(string(out)) {
		fifoStreams = 10
	}

	var fifoDir string
	if fifoStreams > 0 {
		var err error
		fifoDir, err = os.MkdirTemp("", "mysql-restore-fifo")
		if err != nil {
			return errors.Wrap(err, "failed to create FIFO tempdir")
		}
		defer func() {
			if err := os.RemoveAll(fifoDir); err != nil {
				j.l.WithError(err).Warn("failed to remove FIFO temporary directory")
			}
		}()

		j.l.Infof("Using FIFO datasink with %d streams.", fifoStreams)
	}

	xbcloudCmd, xbstreamCmd, err := prepareRestoreCommands(
		pipeCtx,
		artifactFolder,
		&j.locationConfig,
		targetDirectory,
		fifoDir,
		fifoStreams,
		&stderr,
		&stdout,
	)
	if err != nil {
		return err
	}

	wrapError := func(err error) error {
		return errors.Wrapf(err, "stderr: %s\n stdout: %s\n", stderr.String(), stdout.String()) //nolint:revive
	}

	if err := xbcloudCmd.Start(); err != nil {
		cancel()
		return errors.Wrap(wrapError(err), "xbcloud start failed")
	}
	defer func() {
		err := xbcloudCmd.Wait()
		if err != nil {
			cancel()
			if rerr != nil {
				rerr = errors.Wrapf(rerr, "xbcloud wait error: %s", err)
			} else {
				rerr = errors.Wrap(wrapError(err), "xbcloud wait failed")
			}
		}
	}()

	if err := xbstreamCmd.Start(); err != nil {
		cancel()
		return errors.Wrap(wrapError(err), "xbstream start failed")
	}
	defer func() {
		err := xbstreamCmd.Wait()
		if err != nil {
			cancel()
			if rerr != nil {
				rerr = errors.Wrapf(rerr, "xbstream wait error: %s", err)
			} else {
				rerr = errors.Wrap(wrapError(err), "xbstream wait failed")
			}
		}
	}()

	return nil
}

func mySQLActive(ctx context.Context, mySQLServiceName string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, systemctlTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "systemctl", "is-active", "--quiet", mySQLServiceName)
	if err := cmd.Start(); err != nil {
		return false, errors.Wrap(err, "starting systemctl is-active command failed")
	}

	// systemctl is-active returns an exit code 0 if service is active, or non-zero otherwise
	var exitError *exec.ExitError
	err := cmd.Wait()
	switch {
	case err == nil:
		return true, nil
	case errors.As(err, &exitError):
		return false, nil
	default:
		return false, errors.WithStack(err)
	}
}

func stopMySQL(ctx context.Context, mySQLServiceName string) error {
	ctx, cancel := context.WithTimeout(ctx, systemctlTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "systemctl", "stop", mySQLServiceName)
	err := cmd.Start()
	if err != nil {
		return errors.Wrap(err, "starting systemctl stop command failed")
	}

	return errors.Wrap(cmd.Wait(), "waiting systemctl stop command failed")
}

func startMySQL(ctx context.Context, mySQLServiceName string) error {
	ctx, cancel := context.WithTimeout(ctx, systemctlTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "systemctl", "start", mySQLServiceName)
	err := cmd.Start()
	if err != nil {
		return errors.Wrap(err, "starting systemctl start command failed")
	}

	return errors.Wrap(cmd.Wait(), "waiting systemctl start command failed")
}

func chownRecursive(path string, uid, gid int) error {
	return filepath.Walk(path, func(name string, _ os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		return errors.WithStack(os.Chown(name, uid, gid))
	})
}

// mySQLUserAndGroupIDs returns uid, gid if error is nil.
func mySQLUserAndGroupIDs() (int, int, error) {
	u, err := user.Lookup(mySQLSystemUserName)
	if err != nil {
		return 0, 0, errors.WithStack(err)
	}

	uid, err := strconv.Atoi(u.Uid)
	if err != nil {
		return 0, 0, errors.WithStack(err)
	}

	g, err := user.LookupGroup(mySQLSystemGroupName)
	if err != nil {
		return 0, 0, errors.WithStack(err)
	}

	gid, err := strconv.Atoi(g.Gid)
	if err != nil {
		return 0, 0, errors.WithStack(err)
	}

	return uid, gid, nil
}

func isPathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	switch {
	case err == nil:
		return true, nil
	case os.IsNotExist(err):
		return false, nil
	default:
		return false, errors.WithStack(err)
	}
}

func getPermissions(path string) (os.FileMode, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to get permissions for path: %s", path)
	}
	return info.Mode(), nil
}

func decompressBackup(ctx context.Context, backupDirectory string, compression backupv1.BackupCompression) error {
	if compression == backupv1.BackupCompression_BACKUP_COMPRESSION_NONE {
		return nil
	}
	if output, err := exec.CommandContext( //nolint:gosec
		ctx,
		xtrabackupBin,
		"--decompress",
		"--remove-original",
		"--parallel="+restoreParallelism,
		"--target-dir="+backupDirectory,
	).CombinedOutput(); err != nil {
		return errors.Wrapf(err, "failed to decompress, output: %s", string(output))
	}
	return nil
}

func prepareBackup(ctx context.Context, baseDirectory, incrementalDirectory string, applyLogOnly bool) error {
	args := []string{"--prepare", "--parallel=" + restoreParallelism, "--target-dir=" + baseDirectory}
	if applyLogOnly {
		args = append(args, "--apply-log-only")
	}
	if incrementalDirectory != "" {
		args = append(args, "--incremental-dir="+incrementalDirectory)
	}

	if output, err := exec.CommandContext(ctx, xtrabackupBin, args...).CombinedOutput(); err != nil { //nolint:gosec
		return errors.Wrapf(err, "failed to prepare, output: %s", string(output))
	}
	return nil
}

func restoreBackup(ctx context.Context, backupDirectory, mySQLDirectory string) error {
	// TODO We should implement recognizing correct default permissions based on DB configuration.
	// Setting default value in case the base MySQL folder have been lost.
	mysqlDirPermissions := os.FileMode(0o750)

	exists, err := isPathExists(mySQLDirectory)
	if err != nil {
		return errors.WithStack(err)
	}
	if exists {
		mysqlDirPermissions, err = getPermissions(mySQLDirectory)
		if err != nil {
			return errors.Wrap(err, "failed to get MySQL base directory permissions")
		}
		postfix := ".old" + strconv.FormatInt(time.Now().Unix(), 10)
		err := os.Rename(mySQLDirectory, mySQLDirectory+postfix)
		if err != nil {
			return errors.WithStack(err)
		}
	}

	if output, err := exec.CommandContext( //nolint:gosec
		ctx,
		xtrabackupBin,
		"--copy-back",
		"--datadir="+mySQLDirectory,
		"--target-dir="+backupDirectory,
	).CombinedOutput(); err != nil {
		return errors.Wrapf(err, "failed to copy back, output: %s", string(output))
	}

	uid, gid, err := mySQLUserAndGroupIDs()
	if err != nil {
		return errors.WithStack(err)
	}
	if err := chownRecursive(mySQLDirectory, uid, gid); err != nil {
		return errors.WithStack(err)
	}

	// Set such permissions as original directory has before restoring.
	// If original directory was absent, we set predefined permissions.
	// Permissions inside DB's main directory are managed by xtrabackup utility, and we don't change them.
	if err := os.Chmod(mySQLDirectory, mysqlDirPermissions); err != nil {
		return errors.Wrap(err, "failed to change permissions for MySQL base directory")
	}

	return nil
}

// getMysqlServiceName returns MySQL system service name.
func getMysqlServiceName(ctx context.Context) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, systemctlTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "systemctl", "list-unit-files", "--type=service")
	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", errors.Wrapf(err, "failed to list system services, output: %s", string(output))
	}

	if serviceName := mysqlServiceRegex.Find(output); serviceName != nil {
		return string(serviceName), nil
	}

	return "", errors.New("mysql service not found in the system")
}
