package org.icatproject.site.hzb.ids.storage;

import java.io.IOException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.util.EnumSet;
import java.util.Set;

/*
 * A helper class for certain file operations, needed by both,
 * MainFileStorage and ArchiveFileStorage.
 */
public class FileHelper {

    private Path baseDir;
    private int umask;
    private GroupPrincipal group;

    public FileHelper(Path baseDir, int umask, String groupname)
	throws IOException {
	this.baseDir = baseDir;
	this.umask = umask;
	if (groupname != null) {
	    FileSystem fs = FileSystems.getDefault();
	    UserPrincipalLookupService ls = fs.getUserPrincipalLookupService();
	    this.group = ls.lookupPrincipalByGroupName(groupname);
	} else {
	    this.group = null;
	}
    }

    public static Set<PosixFilePermission> permissionsFromInt(int p) {
	Set<PosixFilePermission> perms 
	    = EnumSet.noneOf(PosixFilePermission.class);
	if ((p & 0400) != 0) {
	    perms.add(PosixFilePermission.OWNER_READ);
	}
	if ((p & 0200) != 0) {
	    perms.add(PosixFilePermission.OWNER_WRITE);
	}
	if ((p & 0100) != 0) {
	    perms.add(PosixFilePermission.OWNER_EXECUTE);
	}
	if ((p & 0040) != 0) {
	    perms.add(PosixFilePermission.GROUP_READ);
	}
	if ((p & 0020) != 0) {
	    perms.add(PosixFilePermission.GROUP_WRITE);
	}
	if ((p & 0010) != 0) {
	    perms.add(PosixFilePermission.GROUP_EXECUTE);
	}
	if ((p & 0004) != 0) {
	    perms.add(PosixFilePermission.OTHERS_READ);
	}
	if ((p & 0002) != 0) {
	    perms.add(PosixFilePermission.OTHERS_WRITE);
	}
	if ((p & 0001) != 0) {
	    perms.add(PosixFilePermission.OTHERS_EXECUTE);
	}
	return perms;
    }

    public Set<PosixFilePermission> getFilePermissions() {
	return permissionsFromInt(0666 & ~umask);
    }

    public Set<PosixFilePermission> getDirPermissions() {
	return permissionsFromInt(0777 & ~umask);
    }

    public void setFilePermissions(Path path) throws IOException {
	if (group != null) {
	    Files.setAttribute(path, "posix:group", group);
	}
	Files.setPosixFilePermissions(path, getFilePermissions());
    }

    public void setDirPermissions(Path dir) throws IOException {
	if (group != null) {
	    Files.setAttribute(dir, "posix:group", group);
	}
	Files.setPosixFilePermissions(dir, getDirPermissions());
    }

    /*
     * Delete a directory and all its parent directories up to, but
     * not including, a baseDir.  Continue deleting parent directories
     * until either a parent is not a proper subdirectory of baseDir
     * or DirectoryNotEmptyException is thrown, whichever comes first.
     */
    public void deleteDirectories(Path dir) throws IOException {
	try {
	    while (true) {
		Path parent = dir.getParent();
		if (!parent.startsWith(baseDir)) {
		    // dir is not a subdirectory of baseDir
		    break;
		}
		Files.delete(dir);
		dir = parent;
	    }
	} catch (DirectoryNotEmptyException e) {
	}
    }

    /*
     * Creates a directory together with all nonexistent parent
     * directories.  Similar to Files.createDirectories(), but also
     * set the correct permissions and group on all the direcories.
     */
    public void createDirectories(Path dir) throws IOException {
	Files.createDirectories(dir);
	try {
	    while (true) {
		Path parent = dir.getParent();
		if (!parent.startsWith(baseDir)) {
		    // dir is not a subdirectory of baseDir
		    break;
		}
		setDirPermissions(dir);
		dir = parent;
	    }
	} catch (FileSystemException e) {
	    // it may happen that some of the parent directories did
	    // already exist and are not owned by the glassfish user.
	    // In this case, ignore the "operation not permitted
	    // error" from setPosixFilePermissions(), assuming that
	    // the permission were already correct for preexisting
	    // directories.
	}
    }

}
