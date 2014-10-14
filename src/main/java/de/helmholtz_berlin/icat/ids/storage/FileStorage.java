package de.helmholtz_berlin.icat.ids.storage;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Files;

import org.icatproject.ids.plugin.DsInfo;

public abstract class FileStorage {

    protected void checkDir(Path dir, File props) throws IOException {
	if (!Files.isDirectory(dir)) {
	    throw new IOException(dir + " as specified in " + props 
				  + " is not a directory");
	}
    }

    protected String getRelPath(DsInfo dsInfo) {
	return dsInfo.getInvName() + "/" + dsInfo.getVisitId() + "/" 
	    + dsInfo.getDsName();
    }

    protected void deleteParentDirs(Path baseDir, Path path) {
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

}
