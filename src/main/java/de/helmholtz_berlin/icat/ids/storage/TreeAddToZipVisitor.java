package de.helmholtz_berlin.icat.ids.storage;

import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;

public class TreeAddToZipVisitor extends SimpleFileVisitor<Path> {

	private List<String> locations = new ArrayList<>();
	private Path baseDir;

	public TreeAddToZipVisitor(Path baseDir) {
		this.baseDir = baseDir;
	}

	@Override
	public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
		locations.add(baseDir.relativize(file).toString());
		return FileVisitResult.CONTINUE;
	}

	public List<String> getLocations() {
		return locations;
	}

}
