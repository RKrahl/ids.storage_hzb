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
 *********************************************************************/

public class DirLock implements Closeable {

    private final static Logger logger 
	= LoggerFactory.getLogger(DirLock.class);

    private String dirname;
    private Path lockf;

    private RandomAccessFile lf;
    private FileLock lock;

    private void acquireLock(boolean shared) throws IOException {
	String lockmode;
	if (shared) {
	    lockmode = "shared";
	} else {
	    lockmode = "exclusive";
	}
	logger.debug("Try to acquire " + lockmode + " lock on " + dirname);
	lf = new RandomAccessFile(lockf.toFile(), "rw");
	lock = lf.getChannel().lock(0L, Long.MAX_VALUE, shared);
	logger.debug("Lock on " + dirname + " acquired");
	Set<PosixFilePermission> rwall = 
	    EnumSet.of(PosixFilePermission.OWNER_READ, 
		       PosixFilePermission.OWNER_WRITE, 
		       PosixFilePermission.GROUP_READ, 
		       PosixFilePermission.GROUP_WRITE, 
		       PosixFilePermission.OTHERS_READ, 
		       PosixFilePermission.OTHERS_WRITE);
	Files.setPosixFilePermissions(lockf, rwall);
    }

    public DirLock(Path dir, boolean shared) throws IOException {
	dirname = dir.toString();
	String name = dir.getFileName().toString();
	lockf = dir.getParent().resolve("." + name + ".lock");
	acquireLock(shared);
	FileTime now = FileTime.fromMillis(System.currentTimeMillis());
	Files.setLastModifiedTime(dir, now);
    }

    public void release() throws IOException {
	logger.debug("Release lock on " + dirname);
	lock.release();
	lf.close();
    }

    public void close() throws IOException {
	release();
    }

    public void deleteLockf() throws IOException {
	Files.delete(lockf);
    }

}

