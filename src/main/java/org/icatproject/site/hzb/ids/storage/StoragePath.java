package org.icatproject.site.hzb.ids.storage;

import java.nio.file.InvalidPathException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.icatproject.ids.plugin.DsInfo;

/*
 * A collection of static helper functions to check file names and to
 * calculate the relative paths of datasets in the storage.
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
	= Pattern.compile("[0-9A-Za-z_][0-9A-Za-z~._+-]*");

    private static Map<String, Pattern> createPropNoMap() {
	Map<String, Pattern> m = new HashMap<String, Pattern>();
	m.put("gate", Pattern.compile("(\\d{3})-\\d{5}-\\d+\\.\\d+-[A-Z]+"));
	m.put("gate2", Pattern.compile("[A-Z]+-(\\d{2,3})-\\d{5}(?:-\\d+\\.\\d+)?"));
	m.put("pub", Pattern.compile("(\\d{2})-ND\\d{6}"));
	m.put("misc", Pattern.compile("(\\d{2})-\\d{6}"));
	return Collections.unmodifiableMap(m);
    }
    
    public static void checkName(String name) throws InvalidPathException {
	if (!nameRegExp.matcher(name).matches()) {
	    throw new InvalidPathException(name, "invalid path element");
	}
    }

    public static String getRelPath(DsInfo dsInfo) throws InvalidPathException {
	String invName = dsInfo.getInvName();
	String dsName = dsInfo.getDsName();
	Matcher im = nameSpaceRegExp.matcher(invName);
        if (!im.matches()) {
            throw new InvalidPathException(invName, "invalid invesigation name");
        }
	String nameSpace = im.group(1);
	String propNo = im.group(2);
	String cycle;
	if (propNoRegExp.containsKey(nameSpace)) {
	    Matcher pm = propNoRegExp.get(nameSpace).matcher(propNo);
	    if (!pm.matches()) {
		throw new InvalidPathException(invName, "invalid invesigation name");
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
