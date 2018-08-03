package org.icatproject.site.hzb.ids.storage;

import java.io.IOException;

import org.icatproject.ids.plugin.DfInfo;
import org.icatproject.ids.plugin.DsInfo;
import org.icatproject.ids.plugin.ZipMapperInterface;

public class ZipMapper implements ZipMapperInterface {

    @Override
    public String getFullEntryName(DsInfo dsInfo, DfInfo dfInfo) 
	throws IOException {
	return "ids/" 
	    + dsInfo.getInvName().replace('/', '_') + "/"
	    + dsInfo.getDsName() + "/" 
	    + dfInfo.getDfName();
    }

    @Override
    public String getFileName(String fullEntryName) {
	int l = -1;
	for (int i = 0; i < 3; i++) {
	    l = fullEntryName.indexOf('/', l + 1);
	}
	return l >= 0 ? fullEntryName.substring(l + 1) : null;
    }

}
