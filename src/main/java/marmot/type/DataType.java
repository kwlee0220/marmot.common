package marmot.type;

import utils.Throwables;

/**
 * 'Serializable'을 상속하는 이유는 spark 때문임.
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class DataType {
	private final String m_name;
	private final TypeCode m_tc;
	private final Class<?> m_instCls;
	
	public static final DataType RESERVED = ReservedType.get();
	public static final ByteType BYTE = ByteType.get();
	public static final ShortType SHORT = ShortType.get();
	public static final IntType INT = IntType.get();
	public static final LongType LONG = LongType.get();
	public static final FloatType FLOAT = FloatType.get();
	public static final DoubleType DOUBLE = DoubleType.get();
	public static final BooleanType BOOLEAN = BooleanType.get();
	public static final StringType STRING = StringType.get();
	public static final BinaryType BINARY = BinaryType.get();
	public static final TypedObjectType TYPED = TypedObjectType.get();
	public static final DateTimeType DATETIME = DateTimeType.get();
	public static final DateType DATE = DateType.get();
	public static final TimeType TIME = TimeType.get();
	public static final DurationType DURATION = DurationType.get();
	public static final IntervalType INTERVAL = IntervalType.get();
	public static final EnvelopeType ENVELOPE = EnvelopeType.get();
	public static final TileType TILE = TileType.get();
	public static final GridCellType GRID_CELL = GridCellType.get();
	public static final PointType POINT = PointType.get();
	public static final MultiPointType MULTI_POINT = MultiPointType.get();
	public static final LineStringType LINESTRING = LineStringType.get();
	public static final MultiLineStringType MULTI_LINESTRING = MultiLineStringType.get();
	public static final PolygonType POLYGON = PolygonType.get();
	public static final MultiPolygonType MULTI_POLYGON = MultiPolygonType.get();
	public static final GeometryCollectionType GEOM_COLLECTION = GeometryCollectionType.get();
	public static final GeometryType GEOMETRY = GeometryType.get();
	public static final TrajectoryType TRAJECTORY = TrajectoryType.get();
	
	/**
	 * 본 데이터 타입의 empty 데이터를 생성한다.
	 * 
	 * @return 데이터 객체.
	 */
	public abstract Object newInstance();
	
	/**
	 * 주어진 스트링 representation을 파싱하여 데이터 객체를 생성한다.
	 * 
	 * @return	데이터 객체
	 */
	public abstract Object parseInstance(String str);
	
	protected DataType(String name, TypeCode tc, Class<?> instClass) {
		m_name = name;
		m_tc = tc;
		m_instCls = instClass;
	}
	
	public final String getName() {
		return m_name;
	}

	public final TypeCode getTypeCode() {
		return m_tc;
	}
	
	public final Class<?> getInstanceClass() {
		return m_instCls;
	}
	
	public boolean isGeometryType() {
		return this instanceof GeometryDataType;
	}
	
	public String toInstanceString(Object instance) {
		try {
			return instance.toString();
		}
		catch ( Exception e ) {
			throw Throwables.toRuntimeException(e);
		}
	}
	
	@Override
	public String toString() {
		return m_name;
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || obj.getClass() != getClass() ) {
			return false;
		}
		
		DataType other = (DataType)obj;
		return m_tc.equals(other.m_tc);
	}
	
	@Override
	public int hashCode() {
		return m_tc.hashCode();
	}
}
