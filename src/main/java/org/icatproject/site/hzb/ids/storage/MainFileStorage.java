package org.icatproject.site.hzb.ids.storage;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.icatproject.ids.plugin.AbstractMainStorage;
import org.icatproject.ids.plugin.AlreadyLockedException;
import org.icatproject.ids.plugin.DsInfo;


public class MainFileStorage extends AbstractMainStorage {

    private static final Logger logger
	= LoggerFactory.getLogger(MainFileStorage.class);

    public static final Pattern locationPrefixRegExp 
	= Pattern.compile("([A-Za-z]+):([0-9A-Za-z./_~+-]+)");

    private Path baseDir;
    private FileHelper fileHelper;
    private Map<String, Path> extBaseDirs;
    private boolean doFileLocking = false;

    public MainFileStorage(Properties properties) throws IOException {
	CheckedProperties props = new CheckedProperties(properties);
	baseDir = props.getDirectory("plugin.main.dir");
	int umask = props.getOctalNumber("plugin.main.umask");
	String group;
	if (props.has("plugin.main.group")) {
	    group = props.getString("plugin.main.group");
	} else {
	    group = null;
	}
	fileHelper = new FileHelper(baseDir, umask, group);
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

    private String getRelPath(DsInfo dsInfo) throws InvalidPathException {
	return StoragePath.getRelPath(dsInfo);
    }

    /**
     * Get a Path from a location.
     *
     * If the location contains a storage area prefix, it is resolved
     * relative to the corresponding external storage area directory.
     * Otherwise it is resolved relative to the main storage area.
     */
    public Path getPath(String location) throws InvalidPathException {
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
		throw new InvalidPathException(m.group(1), "unknown storage area");
	    }
	}
	Path localPath = Paths.get(location);
	Path path = base.resolve(localPath);
	if (localPath.isAbsolute() || ! path.equals(path.normalize())) {
	    throw new InvalidPathException(location, "invalid location");
	}
	StoragePath.checkName(path.getFileName().toString());
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
    public Path getMainPath(String location)
	throws IOException, InvalidPathException {
	assertMainLocation(location);
	Path localPath = Paths.get(location);
	Path path = baseDir.resolve(localPath);
	if (localPath.isAbsolute() || ! path.equals(path.normalize())) {
	    throw new InvalidPathException(location, "invalid location");
	}
	StoragePath.checkName(path.getFileName().toString());
	return path;
    }

    @Override
    public Path getPath(String location, String createId, String modId) {
	return getPath(location);
    }

    @Override
    public void delete(DsInfo dsInfo) throws IOException {
	assertMainLocation(dsInfo.getDsLocation());
	Path dir = baseDir.resolve(getRelPath(dsInfo));
	try {
	    if (Files.exists(dir)) {
		TreeDeleteVisitor treeDeleteVisitor = new TreeDeleteVisitor();
		Files.walkFileTree(dir, treeDeleteVisitor);
	    }
	    fileHelper.deleteDirectories(dir.getParent());
	} catch (NoSuchFileException e) {
	}
    }

    @Override
    public void delete(String location, String createId, String modId) 
	throws IOException {
	Path path = getMainPath(location);
	Path dir = path.getParent();
	try {
	    Files.delete(path);
	    Files.delete(dir);
	    fileHelper.deleteDirectories(dir.getParent());
	} catch (DirectoryNotEmptyException | NoSuchFileException e) {
	}
    }

    @Override
    public boolean exists(DsInfo dsInfo) {
	if (checkLocationPrefix(dsInfo.getDsLocation()) != null) {
	    // Datasets in the external storage areas are assumed to
	    // be always ONLINE.
	    return true;
	} else {
	    return Files.exists(baseDir.resolve(getRelPath(dsInfo)));
	}
    }

    @Override
    public boolean exists(String location) {
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
	assertMainLocation(dsInfo.getDsLocation());
	StoragePath.checkName(name);
	String location = getRelPath(dsInfo) + "/" + name;
	Path path = baseDir.resolve(location);
	fileHelper.createDirectories(path.getParent());
	Files.copy(new BufferedInputStream(is), path);
	fileHelper.setFilePermissions(path);
	return location;
    }

    @Override
    public void put(InputStream is, String location) throws IOException {
	Path path = getMainPath(location);
	fileHelper.createDirectories(path.getParent());
	Files.copy(new BufferedInputStream(is), path);
	fileHelper.setFilePermissions(path);
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

    @Override
    public AutoCloseable lock(DsInfo dsInfo, boolean shared)
	throws AlreadyLockedException, IOException {
	if (doFileLocking) {
	    Path dir = baseDir.resolve(getRelPath(dsInfo));
	    fileHelper.createDirectories(dir.getParent());
	    return new DirLock(dir, shared);
	} else {
	    return null;
	}
    }

}
