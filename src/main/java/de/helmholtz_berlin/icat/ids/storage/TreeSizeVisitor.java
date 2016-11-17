package de.helmholtz_berlin.icat.ids.storage;

import java.io.IOException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.icatproject.ids.plugin.AlreadyLockedException;

import de.helmholtz_berlin.icat.ids.storage.DirLock;
import de.helmholtz_berlin.icat.ids.storage.DsInfoImpl;
import de.helmholtz_berlin.icat.ids.storage.FileStorage;

public class TreeSizeVisitor extends SimpleFileVisitor<Path> {

    static Comparator<DsInfoImpl> dateComparator = 
	new Comparator<DsInfoImpl>() {
	@Override
	public int compare(DsInfoImpl o1, DsInfoImpl o2) {
	    FileTime t1 = o1.getLastModifiedTime();
	    FileTime t2 = o2.getLastModifiedTime();
	    return t1.compareTo(t2);
	}
    };

    private Path baseDir;
    private Instant oldLockThreshold;
    private Map<Path, DsInfoImpl> dsInfos = new HashMap<>();
    private long totalSize = 0;

    public TreeSizeVisitor(Path baseDir, long oldLockAge) {
	this.baseDir = baseDir;
	if (oldLockAge > 0) {
	    oldLockThreshold = Instant.now().minusSeconds(oldLockAge);
	} else {
	    oldLockThreshold = Instant.MIN;
	}
    }

    @Override
    public FileVisitResult 
	preVisitDirectory(Path dir, BasicFileAttributes attrs)
	throws IOException {

	Path relPath = baseDir.relativize(dir);
	int numPathEle = relPath.getNameCount();

	if (numPathEle < FileStorage.dsRelPathNameCount) {

	    // ignore directory levels above dataset dirs.
	    return FileVisitResult.CONTINUE;

	} else if (numPathEle == FileStorage.dsRelPathNameCount) {

	    DsInfoImpl dsInfo = new DsInfoImpl(relPath);
	    dsInfo.addSize(attrs.size());
	    dsInfo.updateLastModifiedTime(attrs.lastModifiedTime());
	    dsInfos.put(relPath, dsInfo);
	    totalSize += attrs.size();
	    return FileVisitResult.CONTINUE;

	} else {

	    // there should not be any subdirectories in dataset dirs.
	    throw new IOException("Invalid directory " + dir.toString());

	}

    }

    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) 
	throws IOException {
	Path relPath = baseDir.relativize(file);
	int numPathEle = relPath.getNameCount();

	if (numPathEle < FileStorage.dsRelPathNameCount) {

	    // ignore the upper directory levels.
	    return FileVisitResult.CONTINUE;

	} else if (numPathEle == FileStorage.dsRelPathNameCount) {

	    // one level above dataset dirs: delete old lock files here.
	    // Delete the file if it is older then oldLockThreshold
	    // and is a lock file and if we successfully acquire an
	    // exclusive lock on it and if the corresponding dataset
	    // directory does not exist.  Do nothing otherwise.
	    Instant lastModified = attrs.lastModifiedTime().toInstant();
	    if (lastModified.isBefore(oldLockThreshold)) {
		Path dir = DirLock.getDirPath(file);
		if (dir != null) {
		    try (DirLock lock = new DirLock(dir, false)) {
			if (!dir.toFile().exists()) {
			    Files.delete(file);
			}
		    } catch (AlreadyLockedException e) {
		    }
		}
	    }
	    return FileVisitResult.CONTINUE;

	} else {

	    DsInfoImpl dsInfo = dsInfos.get(relPath.getParent());
	    dsInfo.addSize(attrs.size());
	    dsInfo.updateLastModifiedTime(attrs.lastModifiedTime());
	    totalSize += attrs.size();
	    return FileVisitResult.CONTINUE;

	}

    }

    @Override
    public FileVisitResult postVisitDirectory(Path dir, IOException exc) 
	throws IOException {

	Path relPath = baseDir.relativize(dir);
	int numPathEle = relPath.getNameCount();

	if (exc == null) {
	    if (numPathEle < FileStorage.dsRelPathNameCount && dir != baseDir) {
		// Try to delete empty directories.
		try {
		    Files.delete(dir);
		} catch (DirectoryNotEmptyException e) {
		}
	    }
	    return FileVisitResult.CONTINUE;
	} else {
	    // directory iteration failed
	    throw exc;
	}
    }

    public long getTotalSize() {
	return totalSize;
    }

    public List<DsInfoImpl> getDsInfos() {
	ArrayList<DsInfoImpl> result = new ArrayList<>(dsInfos.values());
	Collections.sort(result, dateComparator);
	return result;
    }

}
