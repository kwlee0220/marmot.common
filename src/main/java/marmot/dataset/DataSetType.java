package marmot.dataset;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public enum DataSetType {
	FILE,
	LINK,
	TEXT,
	CLUSTER,
	GWAVE,
	SPATIAL_CLUSTER;
	
	public String id() {
		return name();
	}

	public static DataSetType fromString(String id) {
		return DataSetType.valueOf(id.toUpperCase());
	}
}
