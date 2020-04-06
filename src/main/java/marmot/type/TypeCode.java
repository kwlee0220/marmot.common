package marmot.type;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public enum TypeCode {
	NULL(0),
	BYTE(1), SHORT(2), INT(3), LONG(4), FLOAT(5),
	DOUBLE(6), BOOLEAN(7), STRING(8), BINARY(9), TYPED(10),
	FLOAT_ARRAY(11), DOUBLE_ARRAY(12), RESERVED13(13), RESERVED14(14), RESERVED15(15),
	DATETIME(16), DATE(17), TIME(18), DURATION(19), INTERVAL(20),
	RESERVED20(21), RESERVED21(22), RESERVED22(23), RESERVED23(24), RESERVED24(25),
	ENVELOPE(26), TILE(27), GRID_CELL(28), RESERVED29(29), RESERVED30(30),
	POINT(31), MULTI_POINT(32), LINESTRING(33), MULTI_LINESTRING(34), POLYGON(35),
	MULTI_POLYGON(36), GEOM_COLLECTION(37), GEOMETRY(38), RESERVED39(39), RESERVED40(40),
	TRAJECTORY(41);
	
	private final byte m_code;
	
	private TypeCode(int code) {
		m_code = (byte)code;
	}
	
	public int get() {
		return m_code;
	}
	
	public static boolean isValid(int code) {
		return code > 0 && code <= TRAJECTORY.m_code;
	}
	
	public static TypeCode fromCode(int code) {
		return values()[code];
	}
}