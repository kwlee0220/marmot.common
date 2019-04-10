package marmot.type;

import static marmot.type.DataType.BINARY;
import static marmot.type.DataType.BOOLEAN;
import static marmot.type.DataType.BYTE;
import static marmot.type.DataType.DATE;
import static marmot.type.DataType.DATETIME;
import static marmot.type.DataType.DOUBLE;
import static marmot.type.DataType.DURATION;
import static marmot.type.DataType.ENVELOPE;
import static marmot.type.DataType.FLOAT;
import static marmot.type.DataType.GEOMETRY;
import static marmot.type.DataType.GEOM_COLLECTION;
import static marmot.type.DataType.GRID_CELL;
import static marmot.type.DataType.INT;
import static marmot.type.DataType.INTERVAL;
import static marmot.type.DataType.LINESTRING;
import static marmot.type.DataType.LONG;
import static marmot.type.DataType.MULTI_LINESTRING;
import static marmot.type.DataType.MULTI_POINT;
import static marmot.type.DataType.MULTI_POLYGON;
import static marmot.type.DataType.POINT;
import static marmot.type.DataType.POLYGON;
import static marmot.type.DataType.RESERVED;
import static marmot.type.DataType.SHORT;
import static marmot.type.DataType.STRING;
import static marmot.type.DataType.TILE;
import static marmot.type.DataType.TIME;
import static marmot.type.DataType.TRAJECTORY;
import static marmot.type.DataType.TYPED;

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
		RESERVED,
		BYTE, SHORT, INT, LONG, FLOAT,
		DOUBLE, BOOLEAN, STRING, BINARY, TYPED,
		RESERVED, RESERVED, RESERVED, RESERVED, RESERVED,
		DATETIME, DATE, TIME, DURATION, INTERVAL,
		RESERVED, RESERVED, RESERVED, RESERVED, RESERVED,
		ENVELOPE, TILE, GRID_CELL,
		RESERVED, RESERVED,
		POINT, MULTI_POINT, LINESTRING, MULTI_LINESTRING, POLYGON,
		MULTI_POLYGON, GEOM_COLLECTION, GEOMETRY,
		RESERVED, RESERVED,
		TRAJECTORY,
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

	public static DataType fromTypeCode(int code) {
		return TYPES[code];
	}

	public static DataType fromTypeCode(TypeCode code) {
		return TYPES[code.get()];
	}

}
