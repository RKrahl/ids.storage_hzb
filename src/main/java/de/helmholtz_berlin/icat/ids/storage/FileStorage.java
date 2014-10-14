package de.helmholtz_berlin.icat.ids.storage;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.icatproject.ids.plugin.DsInfo;

public abstract class FileStorage {

    public static final Pattern invNameRegExp 
	= Pattern.compile("(\\d{3})\\d{5}-[A-Z]+(?:_[A-Z]+)?");
    public static final Pattern visitIdRegExp 
	= Pattern.compile("\\d+\\.\\d+-[A-Z]+(?:_[A-Z]+)?");

    protected void checkDir(Path dir, File props) throws IOException {
	if (!Files.isDirectory(dir)) {
	    throw new IOException(dir + " as specified in " + props 
				  + " is not a directory");
	}
    }

    protected String getRelPath(DsInfo dsInfo) throws IOException {
	String invName = dsInfo.getInvName();
	String visitId = dsInfo.getVisitId();
	String dsName = dsInfo.getDsName();
	Matcher inm = invNameRegExp.matcher(invName.replace('/', '_'));
	Matcher vim = visitIdRegExp.matcher(visitId.replace('/', '_'));
	if (!inm.matches()) {
	    throw new IOException("invalid invesigation name " + invName);
	}
	if (!vim.matches()) {
	    throw new IOException("invalid visit id " + visitId);
	}
	String cycle = inm.group(1);
	return cycle + "/" + invName + "/" + visitId + "/" + dsName;
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
