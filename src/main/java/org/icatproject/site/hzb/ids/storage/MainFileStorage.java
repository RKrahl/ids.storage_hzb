package org.icatproject.site.hzb.ids.storage;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryNotEmptyException;
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
import java.util.Properties;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.icatproject.ids.plugin.DfInfo;
import org.icatproject.ids.plugin.DsInfo;
import org.icatproject.ids.plugin.MainStorageInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MainFileStorage extends FileStorage 
    implements MainStorageInterface {

    private final static Logger logger 
	= LoggerFactory.getLogger(MainFileStorage.class);

    public static final Pattern locationPrefixRegExp 
	= Pattern.compile("([A-Za-z]+):([0-9A-Za-z./_~+-]+)");

    private Map<String, Path> extBaseDirs;
    private boolean doFileLocking = false;

    public MainFileStorage(Properties properties) throws IOException {
	CheckedProperties props = new CheckedProperties(properties);
	baseDir = props.getDirectory("plugin.main.dir");
	umask = props.getOctalNumber("plugin.main.umask");
	if (props.has("plugin.main.filelock")) {
	    doFileLocking = props.getBoolean("plugin.main.filelock");
	}
	extBaseDirs = new HashMap<>();
	if (props.has("plugin.main.extDir.list")) {
	    String extDirlist = props.getString("plugin.main.extDir.list");
	    String[] extDirs = extDirlist.trim().split("\\s+");
	    for (String extDir : extDirs) {
		String propName = "plugin.main.extDir." + extDir + ".dir";
		extBaseDirs.put(extDir, props.getDirectory(propName));
	    }
	}
	logger.info("MainFileStorage initialized");
    }

    /*
     * A do-nothing Closeable.
     */
    private class DummyCloseable implements Closeable {
	DummyCloseable() {
	}
	public void close() {
	}
    }

    private Closeable getDirLock(Path dir, boolean shared) throws IOException {
	if (doFileLocking) {
	    return new DirLock(dir, shared);
	} else {
	    return new DummyCloseable();
	}
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
	Path dir = baseDir.resolve(getRelPath(dsInfo));
	if (Files.exists(dir)) {
	    try (Closeable lock = getDirLock(dir, false)) {
		TreeDeleteVisitor treeDeleteVisitor = new TreeDeleteVisitor();
		Files.walkFileTree(dir, treeDeleteVisitor);
	    }
	}
	deleteDirectories(dir.getParent());
    }

    @Override
    public void delete(String location, String createId, String modId) 
	throws IOException {
	Path path = getMainPath(location);
	Path dir = path.getParent();
	try (Closeable lock = getDirLock(dir, false)) {
	    Files.delete(path);
	    try {
		Files.delete(dir);
	    } catch (DirectoryNotEmptyException e) {
	    }
	}
	deleteDirectories(dir.getParent());
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
	if (doFileLocking && checkLocationPrefix(location) == null) {
	    return new DirLockInputStream(getPath(location));
	} else {
	    return Files.newInputStream(getPath(location));
	}
    }

    @Override
    public String put(DsInfo dsInfo, String name, InputStream is) 
	throws IOException {
	assertMainLocation(dsInfo.getDsLocation());
	checkName(name);
	String location = getRelPath(dsInfo) + "/" + name;
	Path path = baseDir.resolve(location);
	createDirectories(path.getParent());
	try (Closeable lock = getDirLock(path.getParent(), false)) {
	    Files.copy(new BufferedInputStream(is), path);
	    Files.setPosixFilePermissions(path, getFilePermissons());
	}
	return location;
    }

    @Override
    public void put(InputStream is, String location) throws IOException {
	Path path = getMainPath(location);
	createDirectories(path.getParent());
	try (Closeable lock = getDirLock(path.getParent(), false)) {
	    Files.copy(new BufferedInputStream(is), path);
	    Files.setPosixFilePermissions(path, getFilePermissons());
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
	    logger.debug("Size {} < highArchivingLevel {} no action.", 
			 size, highArchivingLevel);
	    return Collections.emptyList();
	}
	long recover = size - lowArchivingLevel;
	logger.debug("Want to reduce size by {}.", recover);

	List<DsInfo> result = new ArrayList<>();
	for (DsInfoImpl dsInfo : treeSizeVisitor.getDsInfos()) {
	    result.add(dsInfo);
	    recover -= dsInfo.getSize();
	    if (recover <= 0) {
		break;
	    }
	}
	logger.debug("{} DsInfos returned to reduce size", result.size());
	return result;
    }

}
