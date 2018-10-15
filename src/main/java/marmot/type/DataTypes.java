package marmot.type;

import java.util.Date;
import java.util.Map;
import java.util.stream.Stream;

import com.google.common.collect.Maps;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DataTypes {
	static DataType[] TYPES = {
		DataType.BYTE, DataType.SHORT, DataType.INT, DataType.LONG,
		DataType.FLOAT, DataType.DOUBLE, DataType.BOOLEAN, DataType.STRING,
		DataType.BINARY, DataType.TYPED,
		DataType.RESERVED, DataType.RESERVED, DataType.RESERVED, DataType.RESERVED,
		DataType.DATETIME, DataType.DATE, DataType.TIME,
		DataType.DURATION, DataType.INTERVAL,
		DataType.RESERVED, DataType.RESERVED, DataType.RESERVED, DataType.RESERVED,
		DataType.RESERVED,
		DataType.ENVELOPE, DataType.TILE, DataType.GRID_CELL,
		DataType.RESERVED, DataType.RESERVED, DataType.RESERVED,
		DataType.RESERVED,
		DataType.POINT, DataType.MULTI_POINT, DataType.LINESTRING, DataType.MULTI_LINESTRING,
		DataType.POLYGON, DataType.MULTI_POLYGON, DataType.GEOM_COLLECTION,
		DataType.GEOMETRY,
		DataType.RESERVED, DataType.RESERVED, DataType.RESERVED, DataType.RESERVED,
		DataType.RESERVED,
		DataType.TRAJECTORY,
	};

	static final Map<Class<?>,DataType> CLASS_TO_TYPES = Maps.newHashMap();
	static final Map<String, DataType> NAME_TO_TYPES = Maps.newHashMap();
	static {
		Stream.of(TYPES)
				.forEach(type -> { 
					NAME_TO_TYPES.put(type.getName(), type);
					CLASS_TO_TYPES.put(type.getInstanceClass(), type);
				});
		CLASS_TO_TYPES.put(Date.class, DataType.DATE);
	}
	
	public static DataType fromInstanceClass(Class<?> cls) {
		DataType type = CLASS_TO_TYPES.get(cls);
		if ( type == null ) {
			throw new IllegalArgumentException("unknown DataType instance class: " + cls);
		}
		
		return type;
	}

	public static DataType fromName(String name) {
		return NAME_TO_TYPES.get(name);
	}

	public static DataType fromTypeCode(byte code) {
		return TYPES[code];
	}

	public static DataType fromTypeCode(TypeCode code) {
		return TYPES[code.ordinal()];
	}

}
