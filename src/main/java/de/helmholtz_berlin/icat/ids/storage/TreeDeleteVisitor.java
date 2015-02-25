package de.helmholtz_berlin.icat.ids.storage;

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
	// Make sure that a lock file, if present, is the very last
	// thing that gets deleted in a directory.  Don't delete it
	// here, but rather in postVisitDirectory().
	if (! file.getFileName().toString().equals(".lock")) {
	    Files.delete(file);
	}
	return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult postVisitDirectory(Path dir, IOException e) 
	throws IOException {
	if (e == null) {
	    // Delete a lock file if present, see above.
	    Path lockf = dir.resolve(".lock");
	    if (Files.exists(lockf)) {
		Files.delete(lockf);
	    }
	    Files.delete(dir);
	    return FileVisitResult.CONTINUE;
	} else {
	    // directory iteration failed
	    throw e;
	}
    }

}
