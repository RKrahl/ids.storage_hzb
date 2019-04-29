package org.icatproject.site.hzb.ids.storage;

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.EnumSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.icatproject.ids.plugin.AlreadyLockedException;

/*********************************************************************
 *
 * class DirLock
 *
 * Set a lock on a directory by acquiring a file lock on a special
 * file in the parent directory.  The lock may either be shared or
 * exclusive.
 *
 *********************************************************************/

public class DirLock implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(DirLock.class);

    private final static Set<PosixFilePermission> allrwPermissions 
	    = PosixFilePermissions.fromString("rw-rw-rw-");

    private String dirname;
    private Path lockf;
    private boolean shared;

    private RandomAccessFile lf;
    private FileLock lock;

    private void acquireLock() throws AlreadyLockedException, IOException {
	logger.debug("Try to acquire a {} lock on {}.",
		     (shared ? "shared" : "exclusive"), dirname);
	lf = new RandomAccessFile(lockf.toFile(), "rw");
	lock = lf.getChannel().tryLock(0L, Long.MAX_VALUE, shared);
	if (lock == null) {
	    throw new AlreadyLockedException();
	}
	logger.debug("Lock on {} acquired.", dirname);
	try {
	    // This may fail if the lockfile has been created by another user.
	    Files.setPosixFilePermissions(lockf, allrwPermissions);
	} catch (IOException e) {
	}
    }

    public DirLock(Path dir, boolean shared) 
	throws AlreadyLockedException, IOException {
	String name = dir.getFileName().toString();
	this.dirname = dir.toString();
	this.lockf = dir.getParent().resolve("." + name + ".lock");
	this.shared = shared;
	acquireLock();
	FileTime now = FileTime.fromMillis(System.currentTimeMillis());
	try {
	    // This may fail if the directory or the lockfile has been
	    // created by another user.
	    if (Files.isDirectory(dir)) {
		// Touch the directory to mark it's recently being accessed.
		// This will be taken into account in
		// MainFileStorage.getDatasetsToArchive()
		Files.setLastModifiedTime(dir, now);
	    }
	    Files.setLastModifiedTime(lockf, now);
	} catch (IOException e) {
	}
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

