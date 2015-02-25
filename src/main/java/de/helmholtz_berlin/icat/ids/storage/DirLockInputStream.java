package de.helmholtz_berlin.icat.ids.storage;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import de.helmholtz_berlin.icat.ids.storage.DirLock;

/*********************************************************************
 *
 * class DirLockInputStream
 *
 * An InputStream that reads from a file and acquires a shared DirLock
 * on the parent directory of the file.  The lock will be released if
 * the InputStream gets closed.
 *
 * Bugs and limitations:
 *
 * + One process can only hold one lock on a directory at a time.  If
 *   a process opens two or more DirLockInputStreams for different
 *   files in the same directory, all of the locks will be released as
 *   soon as the first file is closed.  Unfortunatly this is by design
 *   and cannot be fixed.
 *
 *********************************************************************/

class DirLockInputStream extends InputStream {

    private InputStream is;
    private DirLock lock;

    public DirLockInputStream(Path path) throws IOException {
	lock = new DirLock(path.getParent(), true);
	is = Files.newInputStream(path);
    }

    public int available() throws IOException {
	return is.available();
    }

    public void close() throws IOException {
	is.close();
	lock.release();
    }

    public void mark(int readlimit) {
	is.mark(readlimit);
    }

    public boolean markSupported() {
	return is.markSupported();
    }

    public int read() throws IOException {
	return is.read();
    }

    public int read(byte[] b) throws IOException {
	return is.read(b);
    }

    public int read(byte[] b, int off, int len) throws IOException {
	return is.read(b, off, len);
    }

    public void reset() throws IOException {
	is.reset();
    }

    public long skip(long n) throws IOException {
	return is.skip(n);
    }

}

