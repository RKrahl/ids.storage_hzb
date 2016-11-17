package de.helmholtz_berlin.icat.ids.storage;

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFilePermission;
import java.util.EnumSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.icatproject.ids.plugin.AlreadyLockedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/*********************************************************************
 *
 * class DirLock
 *
 * Set a lock on a directory by acquiring a file lock on a special
 * file in the parent directory.  The lock may either be shared or
 * exclusive.
 *
 * Note that it is practically impossible to safely remove a lock file
 * from the storage without a race condition.  On the other hand, not
 * deleting lock files, leaving them pile up forever in the main
 * storage is no option either.  We ressort to the following strategy:
 * - Remove lock files immediatly before releasing an exclusive lock.
 *   Leave the lock file if we obtained a shared lock.
 * - Always use non-blocking calls to obtain a lock (e.g. tryLock()
 *   rather then lock().
 * This minimizes the risk, but still there is a race condition:
 * 1. A acquires an exclusive lock.
 * 2. B wants to acquire a lock and opens the lock file.
 * 3. A removes the lock file from the file system.
 * 4. A releases the lock.
 * 5. B acquires the lock.  Note that B now holds the lock, but there
 *    is no lock file any more in the file system.
 * 6. C acquires the lock.  Since there is no lock file, C creates a
 *    new one.  Now B and C both hold possibly conflicting locks.
 *
 * Note that this is not a bug in this particular implementation, but
 * rather a design flaw in POSIX fcntl() style locks.  The problem is
 * that fcntl() locks are obtained on open file descriptors and there
 * is no way to open and lock a file in one single atomic operation.
 *
 *********************************************************************/

public class DirLock implements Closeable {

    private final static Logger logger 
	= LoggerFactory.getLogger(DirLock.class);

    public static final Pattern lockFilenameRegExp 
	= Pattern.compile("\\.(.*)\\.lock");

    private String dirname;
    private Path lockf;
    private boolean shared;

    private RandomAccessFile lf;
    private FileLock lock;

    /* 
     * Returns the path of the dataset directory corresponding to a
     * lock file path or null if the given path does not belong to a
     * lock file.
     */
    public static Path getDirPath(Path lockfilePath) {
	String lockname = lockfilePath.getFileName().toString();
	Matcher m = lockFilenameRegExp.matcher(lockname);
	if (m.matches()) {
	    return lockfilePath.getParent().resolve(m.group(1));
	} else {
	    return null;
	}
    }

    private void acquireLock() throws AlreadyLockedException, IOException {
	logger.debug("Try to acquire a {} lock on {}.", 
		     (shared ? "shared" : "exclusive"), dirname);
	Files.createDirectories(lockf.getParent());
	lf = new RandomAccessFile(lockf.toFile(), "rw");
	lock = lf.getChannel().tryLock(0L, Long.MAX_VALUE, shared);
	if (lock == null) {
	    throw new AlreadyLockedException();
	}
	logger.debug("Lock on {} acquired.", dirname);
	Set<PosixFilePermission> rwall = 
	    EnumSet.of(PosixFilePermission.OWNER_READ, 
		       PosixFilePermission.OWNER_WRITE, 
		       PosixFilePermission.GROUP_READ, 
		       PosixFilePermission.GROUP_WRITE, 
		       PosixFilePermission.OTHERS_READ, 
		       PosixFilePermission.OTHERS_WRITE);
	Files.setPosixFilePermissions(lockf, rwall);
    }

    public DirLock(Path dir, boolean shared) 
	throws AlreadyLockedException, IOException {
	String name = dir.getFileName().toString();
	this.dirname = dir.toString();
	this.lockf = dir.getParent().resolve("." + name + ".lock");
	this.shared = shared;
	acquireLock();
	FileTime now = FileTime.fromMillis(System.currentTimeMillis());
	if (Files.isDirectory(dir)) {
	    // Touch the directory to mark it's recently being accessed.
	    // This will be taken into account in
	    // MainFileStorage.getDatasetsToArchive()
	    Files.setLastModifiedTime(dir, now);
	}
	Files.setLastModifiedTime(lockf, now);
    }

    public void release() throws IOException {
	logger.debug("Release lock on {}.", dirname);
	lock.release();
	lf.close();
    }

    public void close() throws IOException {
	release();
    }

}

