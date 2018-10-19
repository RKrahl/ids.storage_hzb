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

    private final static Set<PosixFilePermission> allrwPermissions 
	    = PosixFilePermissions.fromString("rw-rw-rw-");

    private String dirname;
    private Path lockf;
    private boolean shared;

    private RandomAccessFile lf;
    private FileLock lock;

    private void acquireLock() throws AlreadyLockedException, IOException {
	Files.createDirectories(lockf.getParent());
	lf = new RandomAccessFile(lockf.toFile(), "rw");
	lock = lf.getChannel().tryLock(0L, Long.MAX_VALUE, shared);
	if (lock == null) {
	    throw new AlreadyLockedException();
	}
	Files.setPosixFilePermissions(lockf, allrwPermissions);
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
	lock.release();
	lf.close();
    }

    public void close() throws IOException {
	release();
    }

}

