package org.icatproject.site.hzb.ids.storage;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.icatproject.ids.plugin.DsInfo;

/*
 * A collection of static helper functions to check file names and to
 * calculate the relative paths of datasets in the storage.
 *
 * Note: the functions throw IOException on error although
 * IllegalArgumentException would be more appropriate.  But
 * IOException is what MainFileStorage and ArchiveFileStorage are
 * supposed to throw on error and so it's more convenient to avoid the
 * need to translate the exception by the caller.
 */
public class StoragePath {

    // Number of name elements in the relative path down to dataset
    // level.  Must be in sync with the return value of getRelPath().
    public static final int dsRelPathNameCount = 5;

    private static final Pattern nameSpaceRegExp
	= Pattern.compile("([0-9a-z]+):([0-9A-Za-z][0-9A-Za-z~./_+-]+)");
    private static final Map<String, Pattern> propNoRegExp
	= createPropNoMap();
    private static final Pattern nameRegExp
	= Pattern.compile("[0-9A-Za-z][0-9A-Za-z~._+-]*");

    private static Map<String, Pattern> createPropNoMap() {
	Map<String, Pattern> m = new HashMap<String, Pattern>();
	m.put("gate1", Pattern.compile("(\\d{3})\\d{5}-[A-Z]+(?:/[A-Z]+)?-\\d+\\.\\d+-[A-Z]+(?:/[A-Z]+)?"));
	m.put("gate2", Pattern.compile("[A-Z]+-(\\d{3})-\\d+-[A-Z]+(?:/[A-Z]+)?(?:-\\d+\\.\\d+)?"));
	return Collections.unmodifiableMap(m);
    }
    
    public static void checkName(String name) throws IOException {
	if (!nameRegExp.matcher(name).matches()) {
	    throw new IOException("invalid name " + name);
	}
    }

    public static String getRelPath(DsInfo dsInfo) throws IOException {
	String invName = dsInfo.getInvName();
	String dsName = dsInfo.getDsName();
	Matcher im = nameSpaceRegExp.matcher(invName);
        if (!im.matches()) {
            throw new IOException("invalid invesigation name " + invName);
        }
	String nameSpace = im.group(1);
	String propNo = im.group(2);
	String cycle;
	if (propNoRegExp.containsKey(nameSpace)) {
	    Matcher pm = propNoRegExp.get(nameSpace).matcher(propNo);
	    if (!pm.matches()) {
		throw new IOException("invalid invesigation name " + invName);
	    }
	    cycle = pm.group(1);
	} else {
	    cycle = "000";
	}
	checkName(dsName);
	return nameSpace + "/" + cycle 
	    + "/" + propNo.replace('/', '_') 
	    + "/data"
	    + "/" + dsName;
    }

}
