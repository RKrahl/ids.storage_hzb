package org.icatproject.site.hzb.ids.storage;

import java.nio.file.Path;
import java.nio.file.attribute.FileTime;

import org.icatproject.ids.plugin.DsInfo;

public class DsInfoImpl implements DsInfo {

    private Long dsId;
    private String dsLocation;
    private String dsName;
    private Long facilityId;
    private String facilityName;
    private Long invId;
    private String invName;
    private String visitId;
    private long size;
    private FileTime lastModifiedTime;

    /**
     * Initialize the DsInfoImpl from a relative path in the storage.
     * This is in a sense the invers of
     * StoragePath.getRelPath(DsInfo).
     * 
     * The relative paths are of the form
     * nameSpace/cycle/proposalno/data/dsName
     */
    public DsInfoImpl(Path relPath) throws IllegalArgumentException {
	String nameSpace = relPath.getName(0).toString();
	String propNo = relPath.getName(2).toString();
	this.invName = nameSpace + ":" + propNo.replace('_', '/');
	this.dsName = relPath.getName(4).toString();
	this.size = 0;
	this.lastModifiedTime = FileTime.fromMillis(0);
    }

    @Override
    public Long getDsId() {
	return dsId;
    }

    @Override
    public String getDsName() {
	return dsName;
    }

    @Override
    public String getDsLocation() {
	return dsLocation;
    }

    @Override
    public Long getFacilityId() {
	return facilityId;
    }

    @Override
    public String getFacilityName() {
	return facilityName;
    }

    @Override
    public Long getInvId() {
	return invId;
    }

    @Override
    public String getInvName() {
	return invName;
    }

    @Override
    public String getVisitId() {
	return visitId;
    }

    @Override
    public String toString() {
	return invId + "/" + dsId;
    }

    public long getSize() {
	return size;
    }

    public FileTime getLastModifiedTime() {
	return lastModifiedTime;
    }

    public void addSize(long size) {
	this.size += size;
    }

    public void updateLastModifiedTime(FileTime lastModifiedTime) {
	if (this.lastModifiedTime.compareTo(lastModifiedTime) < 0) {
	    this.lastModifiedTime = lastModifiedTime;
	}
    }

}
