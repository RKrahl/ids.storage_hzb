package org.icatproject.site.hzb.ids.storage;

import java.io.IOException;
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
