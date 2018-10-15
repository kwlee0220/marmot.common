package marmot;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.vividsolutions.jts.geom.Geometry;

import marmot.optor.MultiColumnKey;
import marmot.proto.RecordProto;
import marmot.proto.ValueProto;
import marmot.protobuf.PBUtils;
import marmot.support.DataUtils;
import marmot.support.DefaultRecord;
import marmot.support.PBSerializable;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface Record extends Serializable, PBSerializable<RecordProto> {
	/**
	 * 레코드의 스키마 객체를 반환한다.
	 * 
	 * @return	스키마 객체.
	 */
	public RecordSchema getSchema();
	
	public default int getColumnCount() {
		return getSchema().getColumnCount();
	}
	
	public default boolean existsColumn(String name) {
		return getSchema().getColumn(name, null) != null;
	}

	/**
	 * 주어진 순번에 해당하는 컬럼 값을 반환한다.
	 * 
	 * @param index	대상 컬럼 순번.
	 * @return 컬럼 값.
	 * @throws IndexOutOfBoundsException	컬럼 순번이 유효하지 않은 경우.
	 */
	public Object get(int index);

	/**
	 * 컬럼이름에 해당하는 값을 반환한다.
	 * 
	 * @param name	컬럼이름.
	 * @return 컬럼 값.
	 * @throws NoSuchElementException	컬럼이름에 해당하는 컬럼이 존재하지 않는 경우.
	 */
	public Object get(String name);

	/**
	 * 컬럼이름에 해당하는 <p>
	 * 
	 * @param name		컬럼이름.
	 * @param defValue	이름에 해당하는 컬럼이 존재하지 않는 경우 반환될 값.
	 * @return 컬럼 값.
	 */
	public Object get(String name, Object defValue);
	
	/**
	 * 레코드의 모든 컬럼 값들을 리스트 형태로 반환한다.
	 * 리스트의 컬럼 값들은 레코드 스키마에 정의된 순서대로 기록된다.
	 * 
	 * @return	컬럼 값 리스트.
	 */
	public Object[] getAll();
	
	public default Map<String,Object> toMap() {
		Map<String,Object> valueMap = Maps.newLinkedHashMap();
		
		Object[] values = getAll();
		for ( Column col: getSchema().getColumnAll() ) {
			valueMap.put(col.name(), values[col.ordinal()]);
		}
		
		return valueMap;
	}
	
	/**
	 * 이름에 해당하는 컬럼 값을 변경시킨다.
	 * 만일 주어진 이름에 해당하는 컬럼이 없는 경우는 overflow로 저장된다.
	 * 
	 * @param name	컬럼 이름.
	 * @param value	컬럼 값.
	 * @return	갱신된 레코드 객체.
	 */
	public Record set(String name, Object value);
	
	/**
	 * 순번에 해당하는 컬럼 값을 변경시킨다.
	 * 
	 * @param idx	컬럼 순번.
	 * @param value	컬럼 값.
	 * @return	갱신된 레코드 객체.
	 * @throws IndexOutOfBoundsException	컬럼 순번이 유효하지 않은 경우.
	 */
	public Record set(int idx, Object value);
	
	/**
	 * 주어진 레코드({@code src})의 모든 컬럼들을 복사해 온다.
	 * src 레코드에 정의된 모든 컬럼 값들 중에서 본 레코드의 동일 이름의 컬럼이 존재하는 경우
	 * 해당 레코드 값으로 복사한다.
	 * 만일 src 레코드에 overflow 컬럼이 존재하는 경우는 {@code copyOverflow} 인자에 따라
	 * {@code true}인 경우는 복사하고, 그렇지 않은 경우는 복사하지 않는다.
	 * 
	 * @param src	값을 복사해 올 대상 레코드.
	 * @param copyOverflow	src 레코드에 overflow column에 대한 복사 여부.
	 * @return	갱신된 레코드 객체.
	 */
	public Record set(Record src, boolean copyOverflow);
	
	/**
	 * 맵 객체를 이용하여 레코드 컬럼 값을 설정한다.
	 * 맵 객체의 각 (키, 값) 순서쌍에 대해 키와 동일한 이름의 컬럼 값을 설정한다.
	 * 
	 * @param values 	설정할 값을 가진 맵 객체.
	 * @return	갱신된 레코드 객체.
	 */
	public Record set(Map<String,Object> values);
	
	/**
	 * 주어진 레코드의 모든 컬럼들을 복사해 온다.
	 * 
	 * @param values	설정할 컬럼 값.
	 * 					컬럼 값의 순서는 레코드 스크마에 정의된 컬럼 순서와 같아야 한다.
	 * @return	갱신된 레코드 객체.
	 */
	public Record setAll(List<?> values);
	
	/**
	 * 주어진 레코드의 모든 컬럼들을 복사해 온다.
	 * 
	 * @param values	설정할 컬럼 값.
	 * 					컬럼 값의 순서는 레코드 스크마에 정의된 컬럼 순서와 같아야 한다.
	 * @return	갱신된 레코드 객체.
	 */
	public default Record setAll(Object[] values) {
		return setAll(Arrays.asList(values));
	}
	
	public default Record setAll(int start, List<Object> values) {
		IntStream.range(0, values.size())
				.forEach(idx -> set(start+idx, values.get(idx)));
		return this;
	}
	
	public default Record setAll(int start, Object[] values) {
		return setAll(start, Arrays.asList(values));
	}
	
	public void removeOverflow(String name);
	
	public void clear();
	
	/**
	 * 본 레코드를 복사한 레코드를 생성한다.
	 * 
	 * @return	복사된 레코드.
	 */
	public Record duplicate();
	
	public default int getInt(int idx) {
		return DataUtils.asInt(get(idx));
	}
	
	public default int getInt(String name, int defValue) {
		Object value = get(name);
		return value != null ? DataUtils.asInt(value) : defValue;
	}
	
	public default long getLong(int idx) {
		return DataUtils.asLong(get(idx));
	}
	
	public default long getLong(String name, long defValue) {
		Object value = get(name);
		return value != null ? DataUtils.asLong(value) : defValue;
	}
	
	public default short getShort(int idx) {
		return DataUtils.asShort(get(idx));
	}
	
	public default short getShort(String name, short defValue) {
		Object value = get(name);
		return value != null ? DataUtils.asShort(value) : defValue;
	}
	
	public default double getDouble(String name, double defValue) {
		Object value = get(name);
		return value != null ? DataUtils.asDouble(value) : defValue;
	}
	
	public default double getDouble(int idx) {
		return DataUtils.asDouble(get(idx));
	}
	
	public default float getFloat(String name, float defValue) {
		Object value = get(name);
		return value != null ? DataUtils.asFloat(value) : defValue;
	}
	
	public default float getFloat(int idx) {
		return DataUtils.asFloat(get(idx));
	}
	
	public default boolean getBoolean(String name, boolean defValue) {
		Object value = get(name);
		return value != null ? DataUtils.asBoolean(value) : defValue;
	}
	public default boolean getBoolean(int idx) {
		return DataUtils.asBoolean(get(idx));
	}
	
	public default String getString(int idx) {
		Object v = get(idx);
		return v != null ? v.toString() : null;
	}
	
	public default String getString(String name) {
		Object v = get(name);
		return v != null ? v.toString() : null;
	}
	
	public default String getString(String name, String defValue) {
		Object v = get(name, defValue);
		return v != null ? v.toString() : defValue;
	}
	
	public default byte getByte(int idx) {
		return DataUtils.asByte(get(idx));
	}
	
	public default byte getByte(String name, byte defValue) {
		Object value = get(name);
		return value != null ? DataUtils.asByte(value) : defValue;
	}
	
	public default byte[] getBinary(int idx) {
		Preconditions.checkArgument(idx >= 0 && idx < getColumnCount(),
									"invalid column index, idx=" + idx);
		
		Object value = get(idx);
		if ( value instanceof byte[] ) {
			return (byte[])value;
		}
		else {
			String msg = String.format("invalid column index: index='%d', (not binary, but %s)",
										idx, value.getClass());
			throw new IllegalArgumentException(msg);
		}
	}
	
	public default byte[] getBinary(String name) {
		Objects.requireNonNull(name, "column name is null");
		
		Object value = get(name);
		if ( value != null ) {
			if ( value instanceof byte[] ) {
				return (byte[])value;
			}
			else {
				String msg = String.format("invalid column type: name='%s', (not binary, but %s)",
											name, value.getClass());
				throw new IllegalArgumentException(msg);
			}
		}
		else if ( existsColumn(name) ) {
			return null;
		}
		else {
			String msg = String.format("unknown column name: '%s'", name);
			throw new IllegalArgumentException(msg);
		}
	}
	
	public default Geometry getGeometry(int idx) {
		Object v = get(idx);
		return v != null ? (Geometry)v : null;
	}
	
	public default Geometry getGeometry(String name) {
		Object v = get(name);
		return v != null ? (Geometry)v : null;
	}
	
	public default LocalDateTime getDateTime(int idx) {
		Object v = get(idx);
		return v != null ? (LocalDateTime)v : null;
	}
	
	public default LocalDateTime getDateTime(String name) {
		Object v = get(name);
		return v != null ? (LocalDateTime)v : null;
	}
	
	public default void copyTo(Map<String,Object> context) {
		getSchema().forEachIndexedColumn((i,c) -> context.put(c.name(), get(i)));
	}

	/**
	 * 본 레코드에서 주어진 키에 해당하는 컬럼으로 구성된 레코드를 생성한다.
	 * 
	 * @param cols	project할 다중 컬럼 키.
	 * @param projected	키에 포함된 컬럼 이름에 해당하는 컬럼 값들이 채워질 레코드
	 */
	public default void project(MultiColumnKey cols, Record projected) {
		projected.setAll(cols.getKeyColumnAll().stream()
							.map(keyCol -> get(keyCol.name()))
							.collect(Collectors.toList()));
		
	}
	public default Record project(MultiColumnKey cols) {
		Record projected = DefaultRecord.of(getSchema().project(cols));
		project(cols, projected);
		return projected;
	}
	
	public default void fromProto(RecordProto proto) {
		List<Object> values = FStream.of(proto.getValueList())
									.map(vp -> PBUtils.fromProto(vp)._2)
									.toList();
		setAll(values);
	}

	public default RecordProto toProto() {
		RecordProto.Builder builder = RecordProto.newBuilder();
		
		RecordSchema schema = getSchema();
		Object[] values = getAll();
		
		for ( int i =0; i < values.length; ++i ) {
			Column col = schema.getColumnAt(i);
			
			ValueProto vproto = PBUtils.toValueProto(col.type().getTypeCode(), values[i]);
			builder.addValue(vproto);
		}
		
		return builder.build();
	}
}
