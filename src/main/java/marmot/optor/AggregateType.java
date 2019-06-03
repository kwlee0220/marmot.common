package marmot.optor;

import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public enum AggregateType {
	COUNT(0, "count"),
	MAX(1, "max"),
	MIN(2, "min"),
	SUM(3, "sum"),
	AVG(4, "avg"),
	STDDEV(5, "stddev"),
	
	CONVEX_HULL(101, "convex_hull"),
	ENVELOPE(102, "envelope"),
	UNION_GEOM(103, "union_geom"),
	
	CONCAT_STR(201, "concat_str");
	
	private final int m_code;
	private final String m_name;
	
	private AggregateType(int code, String name) {
		m_code = code;
		m_name = name;
	}
	
	public int getCode() {
		return m_code;
	}
	
	public String getName() {
		return m_name;
	}
	
	public static AggregateType fromCode(int code) {
		return FStream.of(values())
						.filter(a -> a.getCode() == code)
						.next()
						.getOrNull();
	}
	
	public static AggregateType fromString(String str) {
		return valueOf(str.toUpperCase());
	}
}