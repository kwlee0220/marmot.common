package marmot.support;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;

import marmot.Column;
import marmot.ColumnNotFoundException;
import marmot.Record;
import marmot.RecordSchema;
import marmot.proto.RecordProto;
import marmot.protobuf.PBUtils;
import utils.Utilities;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DefaultRecord implements Record {
	public static final DefaultRecord NULL = DefaultRecord.of(RecordSchema.NULL);
	private static final Object UNDEFINED = new Object();
	
	private final RecordSchema m_schema;
	private final List<Object> m_values;
	
	public static DefaultRecord of(RecordSchema schema) {
		return new DefaultRecord(schema);
	}
	
	public static DefaultRecord of(RecordSchema schema, List<Object> values) {
		return new DefaultRecord(schema);
	}
	
	protected DefaultRecord(RecordSchema schema) {
		Utilities.checkNotNullArgument(schema, "schema is null");
		
		m_schema = schema;
		m_values = Arrays.asList(new Object[schema.length()]); 
	}
	
	protected DefaultRecord(RecordSchema schema, List<Object> values) {
		Utilities.checkNotNullArgument(schema, "schema is null");
		Utilities.checkNotNullArgument(values, "values is null");
		Utilities.checkArgument(schema.length() == values.size(),
								() -> String.format("imcompatible record values: %d <-> %d",
													schema.length(), values.size()));
		
		m_schema = schema;
		m_values = values;
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
	 * @throws ColumnNotFoundException	컬럼 순번이 유효하지 않은 경우.
	 */
	@Override
	public Object get(int index) {
		if ( index < 0 || index >= m_values.size() ) {
			throw new ColumnNotFoundException("invalid column ordinal: " + index);
		}
		
		return m_values.get(index);
	}

	/**
	 * 컬럼이름에 해당하는 값을 반환한다.
	 * 
	 * @param name	컬럼이름.
	 * @return 컬럼 값.
	 * @throws ColumnNotFoundException	컬럼이름에 해당하는 컬럼이 존재하지 않는 경우.
	 */
	@Override
	public Object get(String name) {
		Utilities.checkNotNullArgument(name, "column name");
		
		return m_values.get(m_schema.getColumn(name).ordinal());
	}
	
	/**
	 * 레코드의 모든 컬럼 값들을 리스트 형태로 반환한다.
	 * 리스트의 컬럼 값들은 레코드 스키마에 정의된 순서대로 기록된다.
	 * 
	 * @return	컬럼 값 리스트.
	 */
	@Override
	public List<Object> getValues() {
		return m_values;
	}
	
	@Override
	public DefaultRecord set(String name, Object value) {
		Utilities.checkNotNullArgument(name, "column name");
		
		Column col = m_schema.getColumn(name);
		m_values.set(col.ordinal(), value);
		
		return this;
	}
	
	/**
	 * 순번에 해당하는 컬럼 값을 변경시킨다.
	 * 
	 * @param idx	컬럼 순번.
	 * @param value	컬럼 값.
	 * @return	갱신된 레코드 객체.
	 * @throws ColumnNotFoundException	컬럼 순번이 유효하지 않은 경우.
	 */
	@Override
	public DefaultRecord set(int idx, Object value) {
		if ( idx < 0 || idx >= m_values.size() ) {
			throw new ColumnNotFoundException("invalid column ordinal: " + idx);
		}
		
		m_values.set(idx, value);
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
	 * @return	갱신된 레코드 객체.
	 */
	@Override
	public DefaultRecord set(Record src) {
		if ( m_schema.equals(src.getRecordSchema()) ) {
			setValues(src.getValues());
		}
		else {
			RecordSchema srcSchema = src.getRecordSchema();
			
			m_schema.streamColumns()
					.forEach(col -> {
						srcSchema.findColumn(col.name())
								.map(srcCol -> src.get(srcCol.ordinal()))
								.ifPresent(srcV -> m_values.set(col.ordinal(), srcV));
					});
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
	public DefaultRecord setValues(Map<String,Object> values) {
		for ( int i =0; i < m_schema.length(); ++i ) {
			final Column col = m_schema.getColumnAt(i);
			
			Object value = values.getOrDefault(col.name(), UNDEFINED);
			if ( value != UNDEFINED ) {
				m_values.set(i, DataUtils.cast(value, col.type()));
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
	public DefaultRecord setValues(Iterable<?> values) {
		Iterator<?> iter = values.iterator();
		for ( int i =0; i < m_values.size() && iter.hasNext(); ++i ) {
			m_values.set(i, iter.next());
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
//	@Override
//	public DefaultRecord setValues(Object... values) {
//		for ( int i =0; i <  Math.min(m_values.size(), values.length); ++i ) {
//			m_values.set(i, values[i]);
//		}
//		return this;
//	}

	@Override
	public DefaultRecord setValues(int start, Object[] values) {
		Preconditions.checkArgument(start >= 0, "invalid start index");
		
		int count = Math.min(m_values.size() - start, values.length);
		for ( int i = 0; i < count; ++i ) {
			m_values.set(i+start, values[i]);
		}
		return this;
	}

	@Override
	public void clear() {
		Collections.fill(m_values, null);
	}
	
	/**
	 * 본 레코드를 복사한 레코드를 생성한다.
	 * 
	 * @return	복사된 레코드.
	 */
	public DefaultRecord duplicate() {
		DefaultRecord copy = DefaultRecord.of(m_schema);
		copy.set(this);
		
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
		return m_values.equals(other.m_values);
	}
	
	@Override
	public String toString() {
		return m_schema.streamColumns()
						.map(Column::name)
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
						.join(",", "[", "]");
	}
	
	public static DefaultRecord fromProto(RecordSchema schema, RecordProto proto) {
		DefaultRecord record = DefaultRecord.of(schema);
		List<Object> columns = FStream.from(proto.getColumnList())
									.map(vp -> PBUtils.fromProto(vp)._2)
									.toList();
		record.setValues(columns);
		
		return record;
	}
}
