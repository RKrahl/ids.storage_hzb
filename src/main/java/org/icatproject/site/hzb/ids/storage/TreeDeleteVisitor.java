package org.icatproject.site.hzb.ids.storage;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

public class TreeDeleteVisitor extends SimpleFileVisitor<Path> {

    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) 
	throws IOException {
	Files.delete(file);
	return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult postVisitDirectory(Path dir, IOException e) 
	throws IOException {
	if (e == null) {
	    Files.delete(dir);
	    return FileVisitResult.CONTINUE;
	} else {
	    // directory iteration failed
	    throw e;
	}
    }

}
