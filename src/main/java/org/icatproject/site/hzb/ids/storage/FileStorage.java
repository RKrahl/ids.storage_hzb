package org.icatproject.site.hzb.ids.storage;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileSystemException;
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

    protected Path baseDir = null;
    protected int umask = 0;

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

    public Set<PosixFilePermission> getDirPermissons() {
	return permissionsFromInt(0777 & ~umask);
    }

    public Set<PosixFilePermission> getFilePermissons() {
	return permissionsFromInt(0666 & ~umask);
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

    /*
     * Delete a directory and all its parent directories.  Continue
     * deleting parent directories until either a parent is not a
     * proper subdirectory of baseDir or DirectoryNotEmptyException is
     * thrown, whichever comes first.
     */
    protected void deleteDirectories(Path dir) throws IOException {
	try {
	    while (true) {
		Path parent = dir.getParent();
		if (!parent.startsWith(baseDir)) {
		    // dir is not a subdirectory of baseDir
		    break;
		}
		Files.delete(dir);
		dir = parent;
	    }
	} catch (DirectoryNotEmptyException e) {
	}
    }

    /*
     * Creates a directory together with all nonexistent parent
     * directories.  Similar to Files.createDirectories(), but also
     * set the correct permissions on all the direcories.
     */
    protected void createDirectories(Path dir) throws IOException {
	Files.createDirectories(dir);
	try {
	    while (true) {
		Path parent = dir.getParent();
		if (!parent.startsWith(baseDir)) {
		    // dir is not a subdirectory of baseDir
		    break;
		}
		Files.setPosixFilePermissions(dir, getDirPermissons());
		dir = parent;
	    }
	} catch (FileSystemException e) {
	    // it may happen that some of the parent directories did
	    // already exist and are not owned by the glassfish user.
	    // In this case, ignore the "operation not permitted
	    // error" from setPosixFilePermissions(), assuming that
	    // the permission were already correct for preexisting
	    // directories.
	}
    }

}
