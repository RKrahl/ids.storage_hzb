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
import java.util.Properties;

import org.icatproject.ids.plugin.AbstractArchiveStorage;
import org.icatproject.ids.plugin.DsInfo;


public class ArchiveFileStorage extends AbstractArchiveStorage {

    private static final int BUFFER_SIZE = 65536;

    private Path baseDir;
    private FileHelper fileHelper;
    private boolean doFileLocking = false;

    public ArchiveFileStorage(Properties properties) throws IOException {
	CheckedProperties props = new CheckedProperties(properties);
	baseDir = props.getDirectory("plugin.archive.dir");
	int umask = props.getOctalNumber("plugin.archive.umask");
	String group;
	if (props.has("plugin.archive.group")) {
	    group = props.getString("plugin.archive.group");
	} else {
	    group = null;
	}
	fileHelper = new FileHelper(baseDir, umask, group);
	if (props.has("plugin.archive.filelock")) {
	    doFileLocking = props.getBoolean("plugin.archive.filelock");
	}
    }

    private String getRelPath(DsInfo dsInfo) throws IOException {
	return StoragePath.getRelPath(dsInfo) + ".zip";
    }

    @Override
    public void delete(DsInfo dsInfo) throws IOException {
	Path path = baseDir.resolve(getRelPath(dsInfo));
	try {
	    Files.delete(path);
	    fileHelper.deleteDirectories(path.getParent());
	} catch (NoSuchFileException e) {
	}
    }

    @Override
    public void put(DsInfo dsInfo, InputStream inputStream) throws IOException {
	Path path = baseDir.resolve(getRelPath(dsInfo));
	fileHelper.createDirectories(path.getParent());
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
	fileHelper.setFilePermissions(path);
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

}
