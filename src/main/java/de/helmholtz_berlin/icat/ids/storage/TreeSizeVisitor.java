package de.helmholtz_berlin.icat.ids.storage;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

public class TreeSizeVisitor extends SimpleFileVisitor<Path> {

    private long size;

    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) 
	throws IOException {
	try {
	    size += Files.size(file);
	} catch (IOException e) {
	    // Ignore it
	}
	return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult postVisitDirectory(Path dir, IOException e) 
	throws IOException {
	if (e == null) {
	    try {
		size += Files.size(dir);
	    } catch (IOException e1) {
		// Ignore it
	    }
	    return FileVisitResult.CONTINUE;
	} else {
	    // directory iteration failed
	    throw e;
	}
    }

    public long getSize() {
	return size;
    }

}
