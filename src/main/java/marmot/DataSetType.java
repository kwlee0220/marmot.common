package marmot;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public enum DataSetType {
	FILE,
	LINK,
	TEXT,
	CLUSTER;
	
	public String id() {
		return name();
	}

	public static DataSetType fromString(String id) {
		return DataSetType.valueOf(id.toUpperCase());
	}
}
