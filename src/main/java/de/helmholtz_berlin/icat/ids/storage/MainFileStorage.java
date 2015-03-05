package de.helmholtz_berlin.icat.ids.storage;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.icatproject.ids.plugin.DfInfo;
import org.icatproject.ids.plugin.DsInfo;
import org.icatproject.ids.plugin.MainStorageInterface;
import org.icatproject.utils.CheckedProperties;
import org.icatproject.utils.CheckedProperties.CheckedPropertyException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.helmholtz_berlin.icat.ids.storage.DirLock;
import de.helmholtz_berlin.icat.ids.storage.DirLockInputStream;
import de.helmholtz_berlin.icat.ids.storage.FileStorage;


public class MainFileStorage extends FileStorage 
    implements MainStorageInterface {

    private final static Logger logger 
	= LoggerFactory.getLogger(MainFileStorage.class);

    public static final Pattern locationPrefixRegExp 
	= Pattern.compile("([A-Za-z]+):([0-9A-Za-z./_~+-]+)");

    private Path baseDir;
    private Map<String, Path> extBaseDirs;

    public MainFileStorage(File properties) throws IOException {
	try {
	    CheckedProperties props = new CheckedProperties();
	    props.loadFromFile(properties.getPath());
	    baseDir = props.getFile("dir").toPath();
	    checkDir(baseDir, properties);

	    extBaseDirs = new HashMap<>();
	    String extDirlist = props.getProperty("extDir.list");
	    if (!(extDirlist == null || extDirlist.trim().isEmpty())) {
		String[] extDirs = extDirlist.trim().split("\\s+");
		for (String extDir : extDirs) {
		    String propName = "extDir." + extDir + ".dir";
		    Path dir = props.getFile(propName).toPath();
		    checkDir(dir, properties);
		    extBaseDirs.put(extDir, dir);
		}
	    }
	} catch (CheckedPropertyException e) {
	    throw new IOException("CheckedPropertException " + e.getMessage());
	}
	logger.info("MainFileStorage initialized");
    }

    protected MatchResult checkLocationPrefix(String location) {
	if (location == null) {
	    return null;
	}
	Matcher m = locationPrefixRegExp.matcher(location);
	if (m.matches()) {
	    return m;
	} else {
	    return null;
	}
    }

    protected void assertMainLocation(String location) throws IOException {
	MatchResult m = checkLocationPrefix(location);
	if (m != null) {
	    throw new IOException("write access to external storage area " 
				  + m.group(1) + " refused");
	}
    }

    /**
     * Delete a directory if it is empty.
     *
     * The argument path must be a directory in the main storage area
     * below baseDir.  If this dierectory is either empty or contains
     * only the lock file, it is deleted together with all parent
     * directories (until one of the parents is not empty).  Do
     * nothing if the directory contains any other file.
     */
    protected void deleteDirIfEmpty(Path dir) throws IOException {
	Path lockf = null;
	try (DirectoryStream<Path> entries = Files.newDirectoryStream(dir)) {
	    for (Path entry: entries) {
		if (entry.getFileName().toString().equals(".lock")) {
		    lockf = entry;
		} else {
		    // found a normal file, dir is not empty, nothing to do.
		    return;
		}
	    }
	}
	if (lockf != null) {
	    Files.delete(lockf);
	}
	Files.delete(dir);
	deleteParentDirs(baseDir, dir);
    }

    /**
     * Get a Path from a location.
     *
     * If the location contains a storage area prefix, it is resolved
     * relative to the corresponding external storage area directory.
     * Otherwise it is resolved relative to the main storage area.
     */
    public Path getPath(String location) throws IOException {
	Path base;
	MatchResult m = checkLocationPrefix(location);
	if (m == null) {
	    // location is in the regular main storage area.
	    base = baseDir;
	} else {
	    // location is in the external storage area as indicated
	    // by the prefix.
	    base = extBaseDirs.get(m.group(1));
	    location = m.group(2);
	    if (base == null) {
		throw new IOException("unknown storage area " + m.group(1));
	    }
	}
	Path localPath = Paths.get(location);
	Path path = base.resolve(localPath);
	if (localPath.isAbsolute() || ! path.equals(path.normalize())) {
	    throw new IOException("invalid location " + location);
	}
	checkName(path.getFileName().toString());
	return path;
    }

    /**
     * Get a Path in the main storage area from a location.
     *
     * Throw an IOException if the location contains a storage area
     * prefix.  Otherwise the location is resolved relative to the
     * main storage area.  This method should be called in the place
     * of getPath() to resolve a location in a context where write
     * access is needed.
     */
    public Path getMainPath(String location) throws IOException {
	assertMainLocation(location);
	Path localPath = Paths.get(location);
	Path path = baseDir.resolve(localPath);
	if (localPath.isAbsolute() || ! path.equals(path.normalize())) {
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
	assertMainLocation(dsInfo.getDsLocation());
	Path path = baseDir.resolve(getRelPath(dsInfo));
	try (DirLock lock = new DirLock(path, false)) {
	    TreeDeleteVisitor treeDeleteVisitor = new TreeDeleteVisitor();
	    if (Files.exists(path)) {
		Files.walkFileTree(path, treeDeleteVisitor);
	    }
	    deleteParentDirs(baseDir, path);
	}
    }

    @Override
    public void delete(String location, String createId, String modId) 
	throws IOException {
	Path path = getMainPath(location);
	try (DirLock lock = new DirLock(path.getParent(), false)) {
	    Files.delete(path);
	    deleteDirIfEmpty(path.getParent());
	}
    }

    @Override
    public boolean exists(DsInfo dsInfo) throws IOException {
	if (checkLocationPrefix(dsInfo.getDsLocation()) != null) {
	    // Datasets in the external storage areas are assumed to
	    // be always ONLINE.
	    return true;
	} else {
	    return Files.exists(baseDir.resolve(getRelPath(dsInfo)));
	}
    }

    @Override
    public boolean exists(String location) throws IOException {
	return Files.exists(getPath(location));
    }

    @Override
    public InputStream get(String location, String createId, String modId) 
	throws IOException {
	if (checkLocationPrefix(location) != null) {
	    return Files.newInputStream(getPath(location));
	} else {
	    return new DirLockInputStream(getPath(location));
	}
    }

    @Override
    public String put(DsInfo dsInfo, String name, InputStream is) 
	throws IOException {
	assertMainLocation(dsInfo.getDsLocation());
	checkName(name);
	String location = getRelPath(dsInfo) + "/" + name;
	Path path = baseDir.resolve(location);
	Files.createDirectories(path.getParent());
	try (DirLock lock = new DirLock(path.getParent(), false)) {
	    Files.copy(new BufferedInputStream(is), path);
	}
	return location;
    }

    @Override
    public void put(InputStream is, String location) throws IOException {
	Path path = getMainPath(location);
	Files.createDirectories(path.getParent());
	try (DirLock lock = new DirLock(path.getParent(), false)) {
	    Files.copy(new BufferedInputStream(is), path);
	}
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
