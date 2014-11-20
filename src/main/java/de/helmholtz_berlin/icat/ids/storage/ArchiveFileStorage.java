package de.helmholtz_berlin.icat.ids.storage;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Set;

import org.icatproject.ids.plugin.ArchiveStorageInterface;
import org.icatproject.ids.plugin.DfInfo;
import org.icatproject.ids.plugin.DsInfo;
import org.icatproject.ids.plugin.MainStorageInterface;
import org.icatproject.utils.CheckedProperties;
import org.icatproject.utils.CheckedProperties.CheckedPropertyException;

import de.helmholtz_berlin.icat.ids.storage.FileStorage;

public class ArchiveFileStorage extends FileStorage 
    implements ArchiveStorageInterface {

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

    @Override
    protected String getRelPath(DsInfo dsInfo) throws IOException {
	return super.getRelPath(dsInfo) + ".zip";
    }

    @Override
    public void delete(DsInfo dsInfo) throws IOException {
	Path path = baseDir.resolve(getRelPath(dsInfo));
	Files.delete(path);
	deleteParentDirs(baseDir, path);
    }

    @Override
    public void delete(String location) throws IOException {
	throw new IOException("This plugin does not support StorageUnit \"DATAFILE\"");
    }

    @Override
    public void put(DsInfo dsInfo, InputStream inputStream) throws IOException {
	Path path = baseDir.resolve(getRelPath(dsInfo));
	Files.createDirectories(path.getParent());
	Files.copy(inputStream, path, StandardCopyOption.REPLACE_EXISTING);
    }

    @Override
    public void put(InputStream inputStream, String location) 
	throws IOException {
	throw new IOException("This plugin does not support StorageUnit \"DATAFILE\"");
    }

    @Override
    public void get(DsInfo dsInfo, Path path) throws IOException {
	String location = getRelPath(dsInfo);
	String inpath = baseDir.resolve(location).toString();
	try (FileInputStream in = new FileInputStream(inpath)) {
	    FileChannel inch = in.getChannel();
	    try (FileLock lock = inch.lock(0L, Long.MAX_VALUE, true)) {
		Files.copy(in, path, 
			   StandardCopyOption.REPLACE_EXISTING);
	    }
	}
    }

    @Override
    public Set<DfInfo> restore(MainStorageInterface mainStorageInterface, 
			       List<DfInfo> dfInfos)
	throws IOException {
	throw new IOException("This plugin does not support StorageUnit \"DATAFILE\"");
    }

}
