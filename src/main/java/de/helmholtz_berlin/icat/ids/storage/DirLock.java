package de.helmholtz_berlin.icat.ids.storage;

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/*********************************************************************
 *
 * class DirLock
 *
 * Set a lock on a directory by acquiring a file lock on a special
 * file in this directory.  The lock may either be shared or
 * exclusive.
 *
 *********************************************************************/

public class DirLock implements Closeable {

    private final static Logger logger 
	= LoggerFactory.getLogger(DirLock.class);

    private RandomAccessFile lf;
    private FileLock lock;

    private String dirname;

    private void acquireLock(Path lockf, boolean shared) throws IOException {
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

	FileTime now = FileTime.fromMillis(System.currentTimeMillis());
	Files.setLastModifiedTime(lockf, now);
    }

    public DirLock(Path dir, boolean shared, Path fname) throws IOException {
	dirname = dir.toString();
	acquireLock(dir.resolve(fname), shared);
    }

    public DirLock(Path dir, boolean shared) throws IOException {
	dirname = dir.toString();
	acquireLock(dir.resolve(".lock"), shared);
    }

    public void release() throws IOException {
	logger.debug("Release lock on " + dirname);
	lock.release();
	lf.close();
    }

    public void close() throws IOException {
	release();
    }

}

