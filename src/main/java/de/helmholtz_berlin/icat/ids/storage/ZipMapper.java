package de.helmholtz_berlin.icat.ids.storage;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

import org.icatproject.ids.plugin.DfInfo;
import org.icatproject.ids.plugin.DsInfo;
import org.icatproject.ids.plugin.ZipMapperInterface;

/**
 * An implementation of the ZipMapperInterface that encodes the parts of the name using percentage
 * encoding. This ensures that the name is file system friendly when unzipped. However the last part
 * which may be expected to include '/' characters is not encoded. This assumes that the user has
 * given some thought to the choice of file name which is obtained from the field Datafile.name.
 */
public class ZipMapper implements ZipMapperInterface {

	@Override
	public String getFullEntryName(DsInfo dsInfo, DfInfo dfInfo) throws IOException {
		try {
			return "ids/" + URLEncoder.encode(dsInfo.getFacilityName(), "UTF-8") + "/"
					+ URLEncoder.encode(dsInfo.getInvName(), "UTF-8") + "/"
					+ URLEncoder.encode(dsInfo.getVisitId(), "UTF-8") + "/"
					+ URLEncoder.encode(dsInfo.getDsName(), "UTF-8") + "/" + dfInfo.getDfName();
		} catch (UnsupportedEncodingException e) {
			throw new IOException(e.getClass() + " " + e.getMessage());
		}
	}

	@Override
	public String getFileName(String fullEntryName) {
		int l = -1;
		for (int i = 0; i < 5; i++) {
			l = fullEntryName.indexOf('/', l + 1);
		}
		return l >= 0 ? fullEntryName.substring(l + 1) : null;
	}

}
