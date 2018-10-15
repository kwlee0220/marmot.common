package marmot.geo.catalog;

import java.util.Objects;

import com.google.common.base.Preconditions;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Catalogs {
	public static final char ID_DELIM = '/';
	
	public static String normalize(String dsId) {
		Objects.requireNonNull(dsId, "dataset is is null");
		
		dsId = dsId.trim();
		if ( dsId.charAt(0) != ID_DELIM ) {
			dsId = ID_DELIM + dsId;
		}
		if ( dsId.charAt(dsId.length()-1) == ID_DELIM ) {
			dsId = dsId.substring(0, dsId.length()-1);
		}
		
		return dsId;
	}
	
	public static String toDataSetId(String folder, String name) {
		Objects.requireNonNull(folder, "dataset folder is is null");
		Objects.requireNonNull(name, "id is is null");
		Preconditions.checkArgument(name.indexOf(ID_DELIM) >= 0, "invalid name: " + name);
		
		return normalize(folder) + ID_DELIM + name;
	}
	
	public static String getFolder(String dsId) {
		dsId = normalize(dsId);
		
		int idx = dsId.lastIndexOf(ID_DELIM);
		return (idx > 0) ? dsId.substring(0, idx) : "/";
	}
}
