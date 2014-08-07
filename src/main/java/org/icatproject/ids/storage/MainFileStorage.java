package org.icatproject.ids.storage;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

import org.icatproject.ids.plugin.DsInfo;
import org.icatproject.ids.plugin.MainStorageInterface;
import org.icatproject.utils.CheckedProperties;
import org.icatproject.utils.CheckedProperties.CheckedPropertyException;

public class MainFileStorage implements MainStorageInterface {

	Path baseDir;

	static Comparator<File> dateComparator = new Comparator<File>() {

		@Override
		public int compare(File o1, File o2) {
			long m1 = o1.lastModified();
			long m2 = o2.lastModified();
			return (m1 < m2) ? -1 : ((m1 == m2) ? 0 : 1);
		}
	};

	public MainFileStorage(File properties) throws IOException {
		try {
			CheckedProperties props = new CheckedProperties();
			props.loadFromFile(properties.getPath());

			baseDir = props.getFile("dir").toPath();
			checkDir(baseDir, properties);

		} catch (CheckedPropertyException e) {
			throw new IOException("CheckedPropertException " + e.getMessage());
		}
	}

	private void checkDir(Path dir, File props) throws IOException {
		if (!Files.isDirectory(dir)) {
			throw new IOException(dir + " as specified in " + props + " is not a directory");
		}
	}

	@Override
	public void delete(DsInfo dsInfo) throws IOException {
		Path path = baseDir.resolve(getRelPath(dsInfo));
		TreeDeleteVisitor treeDeleteVisitor = new TreeDeleteVisitor();
		if (Files.exists(path)) {
			Files.walkFileTree(path, treeDeleteVisitor);
		}
		/* Try deleting empty directories */
		path = path.getParent();
		try {
			while (!path.equals(baseDir)) {
				Files.delete(path);
				path = path.getParent();
			}
		} catch (IOException e) {
			// Directory probably not empty
		}

	}

	@Override
	public void delete(String location) throws IOException {
		Path path = baseDir.resolve(location);
		Files.delete(path);
		/* Try deleting empty directories */
		path = path.getParent();
		try {
			while (!path.equals(baseDir)) {
				Files.delete(path);
				path = path.getParent();
			}
		} catch (IOException e) {
			// Directory probably not empty
		}
	}

	@Override
	public boolean exists(DsInfo dsInfo) throws IOException {
		return Files.exists(baseDir.resolve(getRelPath(dsInfo)));
	}

	@Override
	public InputStream get(String location, String createId, String modId) throws IOException {
		return Files.newInputStream(baseDir.resolve(location));
	}

	@Override
	public String put(DsInfo dsInfo, String name, InputStream is) throws IOException {
		String location = getRelPath(dsInfo) + "/" + UUID.randomUUID();
		Path path = baseDir.resolve(location);
		Files.createDirectories(path.getParent());
		Files.copy(new BufferedInputStream(is), path);
		return location;
	}

	private String getRelPath(DsInfo dsInfo) {
		return dsInfo.getInvId() + "/" + dsInfo.getDsId();
	}

	@Override
	public void put(InputStream is, String location) throws IOException {
		Path path = baseDir.resolve(location);
		Files.createDirectories(path.getParent());
		Files.copy(new BufferedInputStream(is), path);
	}

	@Override
	public long getUsableSpace() throws IOException {
		return baseDir.toFile().getUsableSpace();
	}

	@Override
	public List<Long> getInvestigations() throws IOException {
		List<File> files = Arrays.asList(baseDir.toFile().listFiles());
		Collections.sort(files, dateComparator);
		List<Long> results = new ArrayList<>(files.size());
		for (File file : files) {
			results.add(Long.parseLong(file.getName()));
		}
		return results;
	}

	@Override
	public List<Long> getDatasets(long invId) throws IOException {
		List<File> files = Arrays
				.asList(baseDir.resolve(Long.toString(invId)).toFile().listFiles());
		Collections.sort(files, dateComparator);
		List<Long> results = new ArrayList<>(files.size());
		for (File file : files) {
			results.add(Long.parseLong(file.getName()));
		}
		return results;
	}

	@Override
	public Path getPath(String location, String createId, String modId) throws IOException {
		return baseDir.resolve(location);
	}

}
