package de.helmholtz_berlin.icat.ids.storage;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import de.helmholtz_berlin.icat.ids.storage.DsInfoImpl;

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
    private Map<Path, DsInfoImpl> dsInfos = new HashMap<>();
    private long totalSize = 0;

    public TreeSizeVisitor(Path baseDir) {
	this.baseDir = baseDir;
    }

    @Override
    public FileVisitResult 
	preVisitDirectory(Path dir, BasicFileAttributes attrs)
	throws IOException {

	Path relPath = baseDir.relativize(dir);
	int numPathEle = relPath.getNameCount();

	if (numPathEle < 5) {

	    // ignore directory levels above dataset dirs.
	    return FileVisitResult.CONTINUE;

	} else if (numPathEle == 5) {

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

	if (numPathEle <= 5) {

	    // ignore directory levels above dataset dirs.
	    return FileVisitResult.CONTINUE;

	} else {

	    DsInfoImpl dsInfo = dsInfos.get(relPath.getParent());
	    dsInfo.addSize(attrs.size());
	    dsInfo.updateLastModifiedTime(attrs.lastModifiedTime());
	    totalSize += attrs.size();
	    return FileVisitResult.CONTINUE;

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
