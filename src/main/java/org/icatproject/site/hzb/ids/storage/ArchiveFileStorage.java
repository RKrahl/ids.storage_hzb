package org.icatproject.site.hzb.ids.storage;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.icatproject.ids.plugin.ArchiveStorageInterface;
import org.icatproject.ids.plugin.DfInfo;
import org.icatproject.ids.plugin.DsInfo;
import org.icatproject.ids.plugin.MainStorageInterface;


public class ArchiveFileStorage extends FileStorage 
    implements ArchiveStorageInterface {

    private static final int BUFFER_SIZE = 65536;

    private boolean doFileLocking = false;

    public ArchiveFileStorage(Properties properties) throws IOException {
	CheckedProperties props = new CheckedProperties(properties);
	baseDir = props.getDirectory("plugin.archive.dir");
	umask = props.getOctalNumber("plugin.archive.umask");
	if (props.has("plugin.archive.group")) {
	    group = props.getGroupPrincipal("plugin.archive.group");
	}
	if (props.has("plugin.archive.filelock")) {
	    doFileLocking = props.getBoolean("plugin.archive.filelock");
	}
    }

    @Override
    protected String getRelPath(DsInfo dsInfo) throws IOException {
	return super.getRelPath(dsInfo) + ".zip";
    }

    @Override
    public void delete(DsInfo dsInfo) throws IOException {
	Path path = baseDir.resolve(getRelPath(dsInfo));
	try {
	    Files.delete(path);
	    deleteDirectories(path.getParent());
	} catch (NoSuchFileException e) {
	}
    }

    @Override
    public void delete(String location) throws IOException {
	throw new IOException("This plugin does not support StorageUnit \"DATAFILE\" (ArchiveFileStorage.delete())");
    }

    @Override
    public void put(DsInfo dsInfo, InputStream inputStream) throws IOException {
	Path path = baseDir.resolve(getRelPath(dsInfo));
	createDirectories(path.getParent());
	if (doFileLocking) {
	    try (FileOutputStream out = new FileOutputStream(path.toString())) {
		FileChannel ch = out.getChannel();
		try (FileLock lock = ch.lock()) {
		    byte[] buffer = new byte[BUFFER_SIZE];
		    int len;
		    while ((len = inputStream.read(buffer)) > -1) {
			out.write(buffer, 0, len);
		    }
		}
	    }
	} else {
	    Files.copy(inputStream, path, StandardCopyOption.REPLACE_EXISTING);
	}
	if (group != null) {
	    Files.setAttribute(path, "posix:group", group);
	}
	Files.setPosixFilePermissions(path, getFilePermissons());
    }

    @Override
    public void put(InputStream inputStream, String location) 
	throws IOException {
	throw new IOException("This plugin does not support StorageUnit \"DATAFILE\" (ArchiveFileStorage.put())");
    }

    @Override
    public void get(DsInfo dsInfo, Path path) throws IOException {
	String location = getRelPath(dsInfo);
	String inpath = baseDir.resolve(location).toString();
	try (FileInputStream in = new FileInputStream(inpath)) {
	    if (doFileLocking) {
		FileChannel inch = in.getChannel();
		try (FileLock lock = inch.lock(0L, Long.MAX_VALUE, true)) {
		    Files.copy(in, path, 
			       StandardCopyOption.REPLACE_EXISTING);
		}
	    } else {
		Files.copy(in, path, 
			   StandardCopyOption.REPLACE_EXISTING);
	    }
	}
    }

    @Override
    public Set<DfInfo> restore(MainStorageInterface mainStorageInterface, 
			       List<DfInfo> dfInfos)
	throws IOException {
	throw new IOException("This plugin does not support StorageUnit \"DATAFILE\" (ArchiveFileStorage.restore())");
    }

}
