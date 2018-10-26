package marmot.optor;

import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public enum AggrType {
	COUNT(0),
	MAX(1),
	MIN(2),
	SUM(3),
	AVG(4),
	STDDEV(5),
	
	CONVEX_HULL(101),
	ENVELOPE(101),
	GEOM_UNION(101),
	
	CONCAT_STR(101);
	
	private final int m_code;
	
	private AggrType(int code) {
		m_code = code;
	}
	
	public int getCode() {
		return m_code;
	}
	
	public static AggrType fromCode(int code) {
		return FStream.of(values())
						.filter(a -> a.getCode() == code)
						.first()
						.getOrNull();
	}
	
	public static AggrType fromString(String str) {
		return valueOf(str.toUpperCase());
	}
}