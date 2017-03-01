package de.helmholtz_berlin.icat.ids.storage;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.EnumSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


import org.icatproject.ids.plugin.DsInfo;

public abstract class FileStorage {

    // Number of name elements in the relative path down to dataset
    // level.  Must be in sync with the return value of getRelPath().
    public static final int dsRelPathNameCount = 6;

    public static final Pattern invNameRegExp 
	= Pattern.compile("(\\d{3})\\d{5}-[A-Z]+(?:/[A-Z]+)?");
    public static final Pattern visitIdRegExp 
	= Pattern.compile("\\d+\\.\\d+-[A-Z]+(?:/[A-Z]+)?");
    public static final Pattern nameRegExp 
	= Pattern.compile("[0-9A-Za-z][0-9A-Za-z~._+-]*");

    int umask = 0;

    /*
     * Helper method to delete all parent directories of a path,
     * assuming the file or directory with this path has already
     * been deleted.  Continue deleting parent directories until
     * either a parent is not a proper subdirectory of baseDir or
     * DirectoryNotEmptyException is thrown, whichever comes first.
     */
    public static void deleteParentDirs(Path baseDir, Path path) 
	throws IOException {
	path = path.getParent();
	try {
	    while (true) {
		Path parent = path.getParent();
		if (!parent.startsWith(baseDir)) {
		    // path is not a subdirectory of baseDir
		    break;
		}
		Files.delete(path);
		path = parent;
	    }
	} catch (DirectoryNotEmptyException e) {
	}
    }

    private static Set<PosixFilePermission> permissionsFromInt(int p) {
	Set<PosixFilePermission> perms 
	    = EnumSet.noneOf(PosixFilePermission.class);
	if ((p & 0400) != 0) {
	    perms.add(PosixFilePermission.OWNER_READ);
	}
	if ((p & 0200) != 0) {
	    perms.add(PosixFilePermission.OWNER_WRITE);
	}
	if ((p & 0100) != 0) {
	    perms.add(PosixFilePermission.OWNER_EXECUTE);
	}
	if ((p & 0040) != 0) {
	    perms.add(PosixFilePermission.GROUP_READ);
	}
	if ((p & 0020) != 0) {
	    perms.add(PosixFilePermission.GROUP_WRITE);
	}
	if ((p & 0010) != 0) {
	    perms.add(PosixFilePermission.GROUP_EXECUTE);
	}
	if ((p & 0004) != 0) {
	    perms.add(PosixFilePermission.OTHERS_READ);
	}
	if ((p & 0002) != 0) {
	    perms.add(PosixFilePermission.OTHERS_WRITE);
	}
	if ((p & 0001) != 0) {
	    perms.add(PosixFilePermission.OTHERS_EXECUTE);
	}
	return perms;
    }

    public void setUmask(String octalMask) throws NumberFormatException {
	umask = Integer.parseInt(octalMask, 8);
    }

    public Set<PosixFilePermission> getDirPermissons() {
	return permissionsFromInt(0777 & ~umask);
    }

    public Set<PosixFilePermission> getFilePermissons() {
	return permissionsFromInt(0666 & ~umask);
    }

    protected void checkDir(Path dir, File props) throws IOException {
	if (!Files.isDirectory(dir)) {
	    throw new IOException(dir + " as specified in " + props 
				  + " is not a directory");
	}
    }

    protected String checkInvName(String invName) throws IOException {
	Matcher m = invNameRegExp.matcher(invName);
	if (!m.matches()) {
	    throw new IOException("invalid invesigation name " + invName);
	}
	return m.group(1);
    }

    protected void checkVisitId(String visitId) throws IOException {
	if (!visitIdRegExp.matcher(visitId).matches()) {
	    throw new IOException("invalid visit id " + visitId);
	}
    }

    protected void checkName(String name) throws IOException {
	if (!nameRegExp.matcher(name).matches()) {
	    throw new IOException("invalid name " + name);
	}
    }

    protected String getRelPath(DsInfo dsInfo) throws IOException {
	String facilityName = dsInfo.getFacilityName();
	String invName = dsInfo.getInvName();
	String visitId = dsInfo.getVisitId();
	String dsName = dsInfo.getDsName();
	String cycle = checkInvName(invName);
	checkVisitId(visitId);
	checkName(dsName);
	checkName(facilityName);
	return facilityName + "/" + cycle 
	    + "/" + invName.replace('/', '_') 
	    + "/" + visitId.replace('/', '_') 
	    + "/data"
	    + "/" + dsName;
    }

}
