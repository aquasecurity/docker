// +build linux

//
// projectquota.go - implements XFS project quota controls
// for setting quota limits on a newly created directory.
// It currently supports the legacy XFS specific ioctls.
//
// TODO: use generic quota control ioctl FS_IOC_FS{GET,SET}XATTR
//       for both xfs/ext4 for kernel version >= v4.5
//

package graphdriver

/*
#include <stdlib.h>
#include <dirent.h>
#include <linux/fs.h>
#include <linux/quota.h>
#include <linux/dqblk_xfs.h>
struct fsxattr {
	__u32		fsx_xflags;
	__u32		fsx_extsize;
	__u32		fsx_nextents;
	__u32		fsx_projid;
	unsigned char	fsx_pad[12];
};
#define FS_XFLAG_PROJINHERIT	0x00000200
#define FS_IOC_FSGETXATTR		_IOR ('X', 31, struct fsxattr)
#define FS_IOC_FSSETXATTR		_IOW ('X', 32, struct fsxattr)

#define PRJQUOTA	2
#define XFS_PROJ_QUOTA	2
#define Q_XSETPQLIM QCMD(Q_XSETQLIM, PRJQUOTA)
#define Q_XGETPQUOTA QCMD(Q_XGETQUOTA, PRJQUOTA)
*/
import "C"
import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"syscall"
	"unsafe"

	"github.com/Sirupsen/logrus"
)

// Quota limit params - currently we only control blocks hard limit
type Quota struct {
	Size uint64
}

// QuotaCtl - Context to be used by storage driver (e.g. overlay)
// who wants to apply project quotas to container dirs
type QuotaCtl struct {
	backingFsBlockDev string
	nextProjectID     uint32
	quotas            map[string]uint32
}

// NewQuotaCtl - initialize project quota support by finding the first project id
// to be used for the next container create.
func NewQuotaCtl(basePath string) (*QuotaCtl, error) {
	//
	// get min project id for next use
	//
	minProjectID, err := getMinProjectID(basePath)
	if err != nil {
		return nil, err
	}

	//
	// create backing filesystem device node
	//
	backingFsBlockDev, err := makeBackingFsDev(basePath)
	if err != nil {
		return nil, err
	}

	logrus.Debugf("NewQuotaCtl(%s): minProjectID = %d", path, minProjectID)

	return &QuotaCtl{
		backingFsBlockDev: backingFsBlockDev,
		nextProjectID:     minProjectID,
		quotas:            make(map[string]uint32),
	}, nil
}

// SetQuota - assign a unique project id to directory and set the quota limits
// for that project id
func (q *QuotaCtl) SetQuota(targetPath string, quota Quota) error {

	projectID, ok := q.quotas[targetPath]
	if !ok {
		projectID = q.nextProjectID

		//
		// assign project id to new container directory
		//
		err := setProjectID(targetPath, projectID)
		if err != nil {
			return err
		}

		q.quotas[targetPath] = projectID
		q.nextProjectID++
	}

	//
	// set the quota limit for the container's project id
	//
	logrus.Debugf("SetQuota(%s, %d): projectID=%d", targetPath, quota.Size, projectID)
	return setProjectQuota(projectID, quota)
}

// setProjectQuota - set the hard and soft quota limits of a project id on xfs
func setProjectQuota(projectID uint32, quota Quota) error {
	var d C.fs_disk_quota_t
	d.d_version = C.FS_DQUOT_VERSION
	d.d_id = C.__u32(projectID)
	d.d_flags = C.XFS_PROJ_QUOTA

	d.d_fieldmask = C.FS_DQ_BHARD | C.FS_DQ_BSOFT
	d.d_blk_hardlimit = C.__u64(quota.Size / 512)
	d.d_blk_softlimit = d.d_blk_hardlimit

	var cs = C.CString(q.backingFsBlockDev)
	defer C.free(unsafe.Pointer(cs))

	_, _, errno := syscall.Syscall6(syscall.SYS_QUOTACTL, C.Q_XSETPQLIM,
		uintptr(unsafe.Pointer(cs)), uintptr(d.d_id),
		uintptr(unsafe.Pointer(&d)), 0, 0)
	if errno != 0 {
		return fmt.Errorf("Failed to set quota limit for projid %d on %s: %v",
			projectID, q.backingFsBlockDev, errno.Error())
	}

	return nil
}

// GetQuota - get the quota limits of a directory that was configured with SetQuota
func (q *QuotaCtl) GetQuota(targetPath string, quota *Quota) error {

	projectID, ok := q.quotas[targetPath]
	if !ok {
		return fmt.Errorf("quota not found for path : %s", targetPath)
	}

	//
	// get the quota limit for the container's project id
	//
	return getProjectQuota(projectID, quota)
}

// getProjectQuota - get the hard quota limits of a project id on xfs
func getProjectQuota(projectID uint32, quota *Quota) error {
	var d C.fs_disk_quota_t

	var cs = C.CString(q.backingFsBlockDev)
	defer C.free(unsafe.Pointer(cs))

	_, _, errno := syscall.Syscall6(syscall.SYS_QUOTACTL, C.Q_XGETPQUOTA,
		uintptr(unsafe.Pointer(cs)), uintptr(C.__u32(projectID)),
		uintptr(unsafe.Pointer(&d)), 0, 0)
	if errno != 0 {
		return fmt.Errorf("Failed to get quota limit for projid %d on %s: %v",
			projectID, q.backingFsBlockDev, errno.Error())
	}
	quota.Size = uint64(d.d_blk_hardlimit) * 512

	return nil
}

// getProjectID - get the project id of path on xfs
func getProjectID(targetPath string) (uint32, error) {
	dir, err := openDir(targetPath)
	if err != nil {
		return 0, err
	}
	defer closeDir(dir)

	var fsx C.struct_fsxattr
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, getDirFd(dir), C.FS_IOC_FSGETXATTR,
		uintptr(unsafe.Pointer(&fsx)))
	if errno != 0 {
		return 0, fmt.Errorf("Failed to get projid for %s: %v", targetPath, errno.Error())
	}

	return uint32(fsx.fsx_projid), nil
}

// setProjectID - set the project id of path on xfs
func setProjectID(targetPath string, projectID uint32) error {
	dir, err := openDir(targetPath)
	if err != nil {
		return err
	}
	defer closeDir(dir)

	var fsx C.struct_fsxattr
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, getDirFd(dir), C.FS_IOC_FSGETXATTR,
		uintptr(unsafe.Pointer(&fsx)))
	if errno != 0 {
		return fmt.Errorf("Failed to get projid for %s: %v", targetPath, errno.Error())
	}
	fsx.fsx_projid = C.__u32(projectID)
	fsx.fsx_xflags |= C.FS_XFLAG_PROJINHERIT
	_, _, errno = syscall.Syscall(syscall.SYS_IOCTL, getDirFd(dir), C.FS_IOC_FSSETXATTR,
		uintptr(unsafe.Pointer(&fsx)))
	if errno != 0 {
		return fmt.Errorf("Failed to set projid for %s: %v", targetPath, errno.Error())
	}

	return nil
}

// getMinProjectID - find the next project id to be used for containers
//
// Return 0 (and error) if project quota is not supported.
// Otherwise the first project quota to use is >=1.
//
// The first thing that getMinProjectID() does is get the project id of the home directory.
// This test will fail if the backing fs is not xfs.
// If backing fs is xfs, but home directory does not have a project id the check will
// succeed and return value of 0 and then the next project id to use will be 1.
// There is also a 'hidden' feature here - project quota can be used to assign a project id
// (using xfs_quota tool) to the driver home directory, e.g.:
//    echo 999:/var/lib/docker/overlay2 >> /etc/projects
//    echo docker:999 >> /etc/projid
//
// In that case, the home directory project id will be used as a "start offset"
// and all containers will be assigned larger project ids (e.g. >= 1000).
// This is a way to prevent xfs_quota management from conflicting with docker.
//
func getMinProjectID(path string) (uint32, error) {
	//
	// Get project id of parent dir as test for project quota support
	//
	minProjectID, err := getProjectID(path)
	if err != nil {
		return 0, err
	}

	//
	// First project id to use for containers is the one after the home project id
	//
	minProjectID++

	files, err := ioutil.ReadDir(path)
	if err != nil {
		return 0, fmt.Errorf("read directory failed : %s", path)
	}
	for _, file := range files {
		if !file.IsDir() {
			continue
		}
		projid, err := getProjectID(filepath.Join(path, file.Name()))
		if err != nil {
			return 0, err
		}
		if minProjectID <= projid {
			minProjectID = projid + 1
		}
	}

	return minProjectID, nil
}

func free(p *C.char) {
	C.free(unsafe.Pointer(p))
}

func openDir(path string) (*C.DIR, error) {
	Cpath := C.CString(path)
	defer free(Cpath)

	dir := C.opendir(Cpath)
	if dir == nil {
		return nil, fmt.Errorf("Can't open dir")
	}
	return dir, nil
}

func closeDir(dir *C.DIR) {
	if dir != nil {
		C.closedir(dir)
	}
}

func getDirFd(dir *C.DIR) uintptr {
	return uintptr(C.dirfd(dir))
}

// Get the backing block device of the driver home directory
// and create a block device node under the home directory
// to be used by quotactl commands
func makeBackingFsDev(home string) (string, error) {
	fileinfo, err := os.Stat(home)
	if err != nil {
		return "", err
	}

	backingFsBlockDev := path.Join(home, "backingFsBlockDev")
	// Re-create just in case comeone copied the home directory over to a new device
	syscall.Unlink(backingFsBlockDev)
	stat := fileinfo.Sys().(*syscall.Stat_t)
	if err := syscall.Mknod(backingFsBlockDev, syscall.S_IFBLK|0600, int(stat.Dev)); err != nil {
		return "", fmt.Errorf("Failed to mknod %s: %v", backingFsBlockDev, err)
	}

	return backingFsBlockDev, nil
}
