package de.helmholtz_berlin.icat.ids.storage;

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

import org.icatproject.ids.plugin.DfInfo;
import org.icatproject.ids.plugin.DsInfo;
import org.icatproject.ids.plugin.MainStorageInterface;
import org.icatproject.utils.CheckedProperties;
import org.icatproject.utils.CheckedProperties.CheckedPropertyException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.helmholtz_berlin.icat.ids.storage.FileStorage;

public class MainFileStorage extends FileStorage 
    implements MainStorageInterface {

    private final static Logger logger 
	= LoggerFactory.getLogger(MainFileStorage.class);

    private Path baseDir;

    public MainFileStorage(File properties) throws IOException {
	try {
	    CheckedProperties props = new CheckedProperties();
	    props.loadFromFile(properties.getPath());
	    baseDir = props.getFile("dir").toPath();
	    checkDir(baseDir, properties);
	} catch (CheckedPropertyException e) {
	    throw new IOException("CheckedPropertException " + e.getMessage());
	}
	logger.info("MainFileStorage initialized");
    }

    public Path getPath(String location) throws IOException {
	Path path = baseDir.resolve(location);
	if (!(path.getParent().startsWith(baseDir) && 
	      path.equals(path.normalize()))) {
	    throw new IOException("invalid location " + location);
	}
	checkName(path.getFileName().toString());
	return path;
    }

    @Override
    public Path getPath(String location, String createId, String modId) 
	throws IOException {
	return getPath(location);
    }

    @Override
    public void delete(DsInfo dsInfo) throws IOException {
	Path path = baseDir.resolve(getRelPath(dsInfo));
	TreeDeleteVisitor treeDeleteVisitor = new TreeDeleteVisitor();
	if (Files.exists(path)) {
	    Files.walkFileTree(path, treeDeleteVisitor);
	}
	deleteParentDirs(baseDir, path);
    }

    @Override
    public void delete(String location, String createId, String modId) 
	throws IOException {
	Path path = getPath(location);
	Files.delete(path);
	deleteParentDirs(baseDir, path);
    }

    @Override
    public boolean exists(DsInfo dsInfo) throws IOException {
	return Files.exists(baseDir.resolve(getRelPath(dsInfo)));
    }

    @Override
    public boolean exists(String location) throws IOException {
	return Files.exists(getPath(location));
    }

    @Override
    public InputStream get(String location, String createId, String modId) 
	throws IOException {
	return Files.newInputStream(getPath(location));
    }

    @Override
    public String put(DsInfo dsInfo, String name, InputStream is) 
	throws IOException {
	checkName(name);
	String location = getRelPath(dsInfo) + "/" + name;
	Path path = baseDir.resolve(location);
	Files.createDirectories(path.getParent());
	Files.copy(new BufferedInputStream(is), path);
	return location;
    }

    @Override
    public void put(InputStream is, String location) throws IOException {
	Path path = getPath(location);
	Files.createDirectories(path.getParent());
	Files.copy(new BufferedInputStream(is), path);
    }

    @Override
    public List<DfInfo> getDatafilesToArchive(long lowArchivingLevel, 
					      long highArchivingLevel)
	throws IOException {
	throw new IOException("This plugin does not support StorageUnit \"DATAFILE\" (MainFileStorage.getDatafilesToArchive())");
    }

    @Override
    public List<DsInfo> getDatasetsToArchive(long lowArchivingLevel, 
					     long highArchivingLevel)
	throws IOException {

	TreeSizeVisitor treeSizeVisitor = new TreeSizeVisitor(baseDir);
	Files.walkFileTree(baseDir, treeSizeVisitor);

	long size = treeSizeVisitor.getTotalSize();
	if (size < highArchivingLevel) {
	    return Collections.emptyList();
	}
	long recover = size - lowArchivingLevel;

	List<DsInfo> result = new ArrayList<>();
	for (DsInfoImpl dsInfo : treeSizeVisitor.getDsInfos()) {
	    result.add(dsInfo);
	    recover -= dsInfo.getSize();
	    if (recover <= 0) {
		break;
	    }
	}
	return result;
    }

}
