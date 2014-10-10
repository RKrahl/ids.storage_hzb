package de.helmholtz_berlin.icat.ids.storage;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

import org.icatproject.ids.plugin.ArchiveStorageInterface;
import org.icatproject.ids.plugin.DsInfo;
import org.icatproject.utils.CheckedProperties;
import org.icatproject.utils.CheckedProperties.CheckedPropertyException;

public class ArchiveFileStorage implements ArchiveStorageInterface {

	Path baseDir;

	public ArchiveFileStorage(File properties) throws IOException {
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

	private String getRelPath(DsInfo dsInfo) {
		return dsInfo.getInvName() + "/" + dsInfo.getVisitId() + "/" + dsInfo.getDsName();
	}

	@Override
	public void delete(DsInfo dsInfo) throws IOException {
		Path path = baseDir.resolve(getRelPath(dsInfo));
		Files.delete(path);
		path = path.getParent();
		try {
			Files.delete(path);
		} catch (IOException e) {
			// Investigation directory probably not empty
		}
	}

	@Override
	public void put(DsInfo dsInfo, InputStream inputStream) throws IOException {
		Path path = baseDir.resolve(getRelPath(dsInfo));
		Files.createDirectories(path.getParent());
		Files.copy(inputStream, path, StandardCopyOption.REPLACE_EXISTING);
	}

	@Override
	public void get(DsInfo dsInfo, Path path) throws IOException {
		String location = getRelPath(dsInfo);
		Files.copy(baseDir.resolve(location), path, StandardCopyOption.REPLACE_EXISTING);
	}

}
