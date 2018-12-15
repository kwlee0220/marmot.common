package marmot.support;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import marmot.Column;
import marmot.Record;
import marmot.RecordSchema;
import marmot.proto.RecordProto;
import marmot.protobuf.PBUtils;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DefaultRecord implements Record {
	public static final DefaultRecord NULL = DefaultRecord.of(RecordSchema.NULL);
	private static final Object UNDEFINED = new Object();
	
	private RecordSchema m_schema;
	private final Object[] m_values;
	private Map<String,Object> m_overflow;
	
	public static DefaultRecord of(RecordSchema schema) {
		return new DefaultRecord(schema);
	}
	
	private DefaultRecord() {
		m_values = new Object[0];
	}
	
	private DefaultRecord(RecordSchema schema) {
		m_schema = schema;
		m_values = new Object[schema.getColumnCount()];
	}
	
	/**
	 * 레코드의 스키마 객체를 반환한다.
	 * 
	 * @return	스키마 객체.
	 */
	@Override
	public RecordSchema getRecordSchema() {
		return m_schema;
	}

	/**
	 * 주어진 순번에 해당하는 컬럼 값을 반환한다.
	 * 
	 * @param index	대상 컬럼 순번.
	 * @return 컬럼 값.
	 * @throws IndexOutOfBoundsException	컬럼 순번이 유효하지 않은 경우.
	 */
	@Override
	public Object get(int index) {
		return m_values[index];
	}

	/**
	 * 컬럼이름에 해당하는 값을 반환한다.
	 * 
	 * @param name	컬럼이름.
	 * @return 컬럼 값.
	 * @throws NoSuchElementException	컬럼이름에 해당하는 컬럼이 존재하지 않는 경우.
	 */
	@Override
	public Object get(String name) {
		Column col = m_schema.getColumn(name, null);
		if ( col != null ) {
			return m_values[col.ordinal()];
		}
		if ( m_overflow != null ) {
System.err.println("OOOOOOOVVVVEEEERRRRR");
			Object value = m_overflow.get(name);
			if ( value != null ) {
				return value;
			}
		}
		
		throw new NoSuchElementException("column name=" + name);
	}

	@Override
	public Object get(String name, Object defValue) {
		Column col = m_schema.getColumn(name, null);
		if ( col != null ) {
			return m_values[col.ordinal()];
		}
		if ( m_overflow != null ) {
System.err.println("OOOOOOOVVVVEEEERRRRR");
			Object value = m_overflow.get(name);
			if ( value != null ) {
				return value;
			}
		}
		
		return defValue;
	}
	
	/**
	 * 레코드의 모든 컬럼 값들을 리스트 형태로 반환한다.
	 * 리스트의 컬럼 값들은 레코드 스키마에 정의된 순서대로 기록된다.
	 * 
	 * @return	컬럼 값 리스트.
	 */
	@Override
	public Object[] getAll() {
		return m_values;
	}
	
	/**
	 * 이름에 해당하는 컬럼 값을 변경시킨다.
	 * 만일 주어진 이름에 해당하는 컬럼이 없는 경우는 overflow로 저장된다.
	 * 
	 * @param name	컬럼 이름.
	 * @param value	컬럼 값.
	 * @return	갱신된 레코드 객체.
	 */
	@Override
	public DefaultRecord set(String name, Object value) {
		Column col = m_schema.getColumn(name, null);
		if ( col != null ) {
			m_values[col.ordinal()] = value;
		}
		else {
			if ( m_overflow == null ) {
				m_overflow = Maps.newHashMap();
			}
System.err.println("OOOOOOOVVVVEEEERRRRR");
			m_overflow.put(name, value);
		}
		return this;
	}
	
	/**
	 * 순번에 해당하는 컬럼 값을 변경시킨다.
	 * 
	 * @param idx	컬럼 순번.
	 * @param value	컬럼 값.
	 * @return	갱신된 레코드 객체.
	 * @throws IndexOutOfBoundsException	컬럼 순번이 유효하지 않은 경우.
	 */
	@Override
	public DefaultRecord set(int idx, Object value) {
		m_values[idx] = value;
		return this;
	}
	
	/**
	 * 주어진 레코드(src))의 모든 컬럼들을 복사해 온다.
	 * src 레코드에 정의된 모든 컬럼 값들 중에서 본 레코드의 동일 이름의 컬럼이 존재하는 경우
	 * 해당 레코드 값으로 복사한다.
	 * 만일 src 레코드에 overflow 컬럼이 존재하는 경우는 {@code copyOverflow} 인자에 따라
	 * {@code true}인 경우는 복사하고, 그렇지 않은 경우는 복사하지 않는다.
	 * 
	 * @param src	값을 복사해 올 대상 레코드.
	 * @param copyOverflow	src 레코드에 overflow column에 대한 복사 여부.
	 * @return	갱신된 레코드 객체.
	 */
	@Override
	public DefaultRecord set(Record src, boolean copyOverflow) {
		if ( getRecordSchema().equals(src.getRecordSchema()) ) {
			setAll(src.getAll());
		}
		else {
			RecordSchema srcSchema = src.getRecordSchema();
			getRecordSchema().forEachIndexedColumn((idx,col) -> {
				Column srcCol = srcSchema.getColumn(col.name(), null);
				if ( srcCol != null ) {
					set(idx, src.get(srcCol.ordinal()));
				}
			});
		}
		
		if ( !copyOverflow || !(src instanceof DefaultRecord) ) {
			return this;
		}
		
		if ( ((DefaultRecord)src).m_overflow != null ) {
System.err.println("OOOOOOOVVVVEEEERRRRR");
			((DefaultRecord)src).m_overflow.entrySet()
							.stream()
							.forEach(ent -> set(ent.getKey(), ent.getValue()));
		}
		
		return this;
	}
	
	/**
	 * 맵 객체를 이용하여 레코드 컬럼 값을 설정한다.
	 * 맵 객체의 각 (키, 값) 순서쌍에 대해 키와 동일한 이름의 컬럼 값을 설정한다.
	 * 
	 * @param values 	설정할 값을 가진 맵 객체.
	 */
	@Override
	public DefaultRecord set(Map<String,Object> values) {
		for ( int i =0; i < m_schema.getColumnCount(); ++i ) {
			final Column col = m_schema.getColumnAt(i);
			
			Object value = values.getOrDefault(col.name(), UNDEFINED);
			if ( value != UNDEFINED ) {
				m_values[i] = DataUtils.cast(value, col.type());
			}
		}
		
		return this;
	}
	
	/**
	 * 주어진 레코드의 모든 컬럼들을 복사해 온다.
	 * 
	 * @param values	설정할 컬럼 값.
	 * 					컬럼 값의 순서는 레코드 스크마에 정의된 컬럼 순서와 같아야 한다.
	 * @return	갱신된 레코드 객체.
	 */
	@Override
	public DefaultRecord setAll(List<?> values) {
		System.arraycopy(values.toArray(), 0, m_values, 0,
						Math.min(m_values.length, values.size()));
		return this;
	}
	
	/**
	 * 주어진 레코드의 모든 컬럼들을 복사해 온다.
	 * 
	 * @param values	설정할 컬럼 값.
	 * 					컬럼 값의 순서는 레코드 스크마에 정의된 컬럼 순서와 같아야 한다.
	 * @return	갱신된 레코드 객체.
	 */
	@Override
	public DefaultRecord setAll(Object[] values) {
		System.arraycopy(values, 0, m_values, 0, Math.min(m_values.length, values.length));
		return this;
	}

	@Override
	public DefaultRecord setAll(int startIdx, Object[] values) {
		Preconditions.checkArgument(startIdx >= 0, "invalid start index");
		
		System.arraycopy(values, 0, m_values, startIdx,
						Math.min(startIdx+m_values.length, values.length));
		return this;
	}

	@Override
	public void removeOverflow(String name) {
System.err.println("OOOOOOOVVVVEEEERRRRR");
		if ( m_overflow != null ) {
			m_overflow.remove(name);
		}
	}

	@Override
	public void clear() {
		for ( int i =0; i < m_values.length; ++i ) {
			m_values[i] = null;
		}
		m_overflow = null;
	}
	
	public DefaultRecord remove(String name) {
		if ( m_overflow.remove(name) != null ) {
System.err.println("OOOOOOOVVVVEEEERRRRR");
			return this;
		}

		Column col = m_schema.getColumn(name, null);
		if ( col != null ) {
			m_values[col.ordinal()] = null;
		}
		
		return this;
	}
	
	public int getInt(String name, int defValue) {
		Object value = get(name, null);
		return value != null ? DataUtils.asInt(value) : defValue;
	}
	
	public long getLong(String name, long defValue) {
		Object value = get(name, null);
		return value != null ? DataUtils.asLong(value) : defValue;
	}
	
	public double getDouble(String name, double defValue) {
		Object value = get(name, null);
		return value != null ? DataUtils.asDouble(value) : defValue;
	}
	
	public boolean getBoolean(String name, boolean defValue) {
		Object value = get(name, null);
		return value != null ? DataUtils.asBoolean(value) : defValue;
	}
	
	public String getString(String name) {
		Object v = get(name);
		return v != null ? v.toString() : null;
	}
	
	/**
	 * 본 레코드를 복사한 레코드를 생성한다.
	 * 
	 * @return	복사된 레코드.
	 */
	public DefaultRecord duplicate() {
		DefaultRecord copy = DefaultRecord.of(m_schema);
		copy.set(this, true);
		
		return copy;
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || !(obj instanceof DefaultRecord) ) {
			return false;
		}
		
		DefaultRecord other = (DefaultRecord)obj;
		return Arrays.equals(m_values, other.getAll());
	}
	
	@Override
	public String toString() {
		Stream<String> colNameStream = m_schema.getColumnNameAll().stream();
		if ( m_overflow != null ) {
			colNameStream = Stream.concat(colNameStream, m_overflow.keySet().stream());
		}
		return colNameStream
					.map(n -> {
						Object v = get(n);
						if ( v == null ) {
							v = "null";
						}
						else if ( v instanceof byte[] ) {
							v = String.format("binary[%d]", ((byte[])v).length);
						}
						return String.format("%s:%s", n, v);
					})
					.collect(Collectors.joining(",", "[", "]"));
	}
	
	public static DefaultRecord fromProto(RecordSchema schema, RecordProto proto) {
		DefaultRecord record = DefaultRecord.of(schema);
		List<Object> columns = FStream.of(proto.getColumnList())
									.map(vp -> PBUtils.fromProto(vp)._2)
									.toList();
		record.setAll(columns);
		
		return record;
	}
}
