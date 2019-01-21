package marmot;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import marmot.proto.ColumnProto;
import marmot.proto.RecordSchemaProto;
import marmot.support.PBSerializable;
import marmot.type.DataType;
import utils.CIString;
import utils.CSV;
import utils.func.FOption;
import utils.stream.FStream;
import utils.stream.KVFStream;


/**
 * {@code RecordSchema}는 {@link RecordSet}에 포함된 레코드들의 스키마 정보를 표현하는
 * 클래스이다.
 * <p>
 * 하나의 레코드 세트에 포함된 모든 레코드는 동일한 레코드 스키마를 갖는다.
 * 레코드 스키마는 레코드를 구성하는 컬럼({@link Column})들의 리스트로 구성되고,
 * 각 컬럼은 이름과 타입으로 구성된다.
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RecordSchema implements PBSerializable<RecordSchemaProto>  {
	/** Empty RecordSchema */
	public static final RecordSchema NULL = builder().build();
	
	private final LinkedHashMap<CIString,Column> m_columns;
	
	public static RecordSchema from(FStream<Column> cols) {
		return cols.foldLeft(builder(), (b,c) -> b.addColumn(c)).build();
	}
	
	private RecordSchema(LinkedHashMap<CIString,Column> columns) {
		m_columns = new LinkedHashMap<>(columns.size());
		for ( Map.Entry<CIString, Column> ent: columns.entrySet() ) {
			m_columns.put(ent.getKey(), ent.getValue());
		}
	}
	
	/**
	 * 주어진 이름의 컬럼의 존재 여부를 반환한다.
	 * 
	 * @param name	대상 컬럼 이름
	 * @return	이름에 해당하는 컬럼이 존재하는 경우는 true, 그렇지 않은 경우는 false
	 */
	public boolean existsColumn(String name) {
		Objects.requireNonNull(name, "column name");
		
		return m_columns.containsKey(CIString.of(name));
	}
	
	/**
	 * 컬럼이름에 해당하는 컬럼 정보를 반환한다.
	 * 
	 * @param name	컬럼이름
	 * @return	컬럼 정보 객체
	 * @throws ColumnNotFoundException	컬럼이름에 해당하는 컬럼이 존재하지 않는 경우
	 */
	public Column getColumn(String name) {
		return findColumn(name)
				.getOrElseThrow(() -> new ColumnNotFoundException("name=" + name
														+ ", schema=" + m_columns.keySet()));
	}
	
	/**
	 * 주어진 이름의 컬럼의 존재 여부를 반환한다.
	 * 만일 해당 이름에 해당하는 컬럼이 존재하지 않는 경우는 {link FOption#empty}를 반환한다.
	 * 
	 * @param name	컬럼이름
	 * @return	컬럼 정보 객체
	 */
	public FOption<Column> findColumn(String name) {
		Objects.requireNonNull(name, "column name");
		
		return FOption.ofNullable(m_columns.get(CIString.of(name)));
	}
	
	/**
	 * 주어진 순번에 해당하는 컬럼 정보를 반환한다.
	 * 
	 * @param idx	컬럼 순번
	 * @return	컬럼 정보 객체
	 */
	public Column getColumnAt(int idx) {
		return Iterables.get(m_columns.values(), idx);
	}
	
	/**
	 * 레코드에 정의된 컬럼의 개수를 반환한다.
	 * 
	 * @return	컬럼 개수
	 */
	public int getColumnCount() {
		return m_columns.size();
	}
	
	/**
	 * 레코드에 정의된 모든 컬럼의 이름 집합을 반환한다.
	 * 
	 * @return	컬럼 이름 집합
	 */
	public Set<String> getColumnNameAll() {
		return FStream.of(m_columns.keySet())
						.map(CIString::get)
						.toCollection(new LinkedHashSet<>());
	}
	
	/**
	 * 레코드에 정의된 모든 컬럼 정보 객체 모음을 반환한다.
	 * 
	 * @return	컬럼 정보 객체 모음
	 * 
	 */
	public Collection<Column> getColumnAll() {
		return Collections.unmodifiableCollection(m_columns.values());
	}
	
	public FStream<Column> getColumnAll(Iterable<String> key) {
		return FStream.of(key).map(this::getColumn);
	}
	
	public FStream<Column> getDifferenceAll(Iterable<String> key) {
		Set<CIString> keyCols = FStream.of(key)
										.map(CIString::of)
										.toHashSet();
		return getColumnStream().filter(col -> !keyCols.contains(col.columnName()));
	}

	/**
	 * 레코드 스키마에 정의된 모든 컬럼 객체 스트림을 반환한다.
	 * 
	 * @return	컬럼 스트림
	 */
	public FStream<Column> getColumnStream() {
		return FStream.of(m_columns.values());
	}
	
	public RecordSchema duplicate() {
		return RecordSchema.builder()
							.addColumnAll(getColumnAll())
							.build();
	}
	
	public void forEachIndexedColumn(IndexedColumnAccessor accessor) {
		Iterator<Column> it = m_columns.values().iterator();
		for ( int i =0; it.hasNext(); ++i ) {
			accessor.access(i, it.next());
		}
	}

	public static RecordSchema fromProto(RecordSchemaProto proto) {
		Objects.requireNonNull(proto, "RecordSchemaProto");
		
		return FStream.of(proto.getColumnsList())
					.map(Column::fromProto)
					.foldLeft(RecordSchema.builder(),
								(b,c)->b.addColumn(c.name(),c.type()))
					.build();
	}
	
	@Override
	public RecordSchemaProto toProto() {
		List<ColumnProto> cols = m_columns.values()
											.stream()
											.map(Column::toProto)
											.collect(Collectors.toList());
		return RecordSchemaProto.newBuilder()
								.addAllColumns(cols)
								.build();
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || obj.getClass() != getClass() ) {
			return false;
		}
		
		RecordSchema other = (RecordSchema)obj;
		if ( m_columns.size() != other.m_columns.size() ) {
			return false;
		}
		
		Collection<Column> cols1 = m_columns.values();
		Collection<Column> cols2 = other.m_columns.values();
		
		return IntStream.range(0, m_columns.size())
						.allMatch(i -> {
							Column c1 = Iterables.get(cols1, i);
							Column c2 = Iterables.get(cols2, i);
							return c1.equals(c2);
						});
	}
	
	public static RecordSchema parse(String schemaStr) {
		return CSV.parseCsv(schemaStr)
					.map(Column::parse)
					.foldLeft(RecordSchema.builder(), (b,c) -> b.addColumn(c))
					.build();
	}
	
	@Override
	public String toString() {
		return getColumnStream().map(Column::toString).join(",");
	}
	
	public interface IndexedColumnAccessor {
		public void access(int ordinal, Column col);
	}
	
	public Builder toBuilder() {
		// 별도 다시 생성하여 사용하지 않으면, column 객체의 공유로 인해
		// 문제가 발생할 수 있음.
		return getColumnStream().foldLeft(builder(), (b,c) -> b.addColumn(c));
	}
	
	public static Builder builder() {
		return new Builder();
	}
	
	public static class Builder {
		private LinkedHashMap<CIString, Column> m_columns;
		
		private Builder() {
			m_columns = Maps.newLinkedHashMap();
		}
		
		public int size() {
			return m_columns.size();
		}
		
		public RecordSchema build() {
			return new RecordSchema(m_columns);
		}
		
		public boolean existsColumn(String name) {
			Objects.requireNonNull(name, "column name");
			
			return m_columns.containsKey(CIString.of(name));
		}
		
		public Builder addColumn(String name, DataType type) {
			Objects.requireNonNull(name, "column name");
			Objects.requireNonNull(type, "column type");
			
			CIString ciName = CIString.of(name);
			Column col = new Column(ciName, type, m_columns.size());
			if ( m_columns.putIfAbsent(ciName, col) != null ) {
				throw new IllegalArgumentException("column already exists: name=" + name);
			}
			return this;
		}
		
		public Builder addColumn(Column col) {
			Objects.requireNonNull(col, "column");
			
			return addColumn(col.name(), col.type());
		}
		
		public Builder addColumnIfAbsent(String name, DataType type) {
			Objects.requireNonNull(name, "column name");
			Objects.requireNonNull(type, "column type");

			CIString cname = CIString.of(name);
			Column col = new Column(cname, type, m_columns.size());
			m_columns.putIfAbsent(cname, col);
			return this;
		}
		
		public Builder addOrReplaceColumn(String name, DataType type) {
			Objects.requireNonNull(name, "column name");
			Objects.requireNonNull(type, "column type");
			
			Column col = m_columns.get(CIString.of(name));
			if ( col != null ) {
				LinkedHashMap<CIString, Column> old = m_columns;
				m_columns = new LinkedHashMap<>(old.size());
				FStream.of(old.values())
						.map(c -> c == col ? new Column(name, type) : c)
						.forEach(this::addColumn);
			}
			else {
				addColumn(name, type);
			}
			
			return this;
		}
		
		public Builder addColumnAll(Column... columns) {
			Objects.requireNonNull(columns, "columns");
			
			for ( Column col: columns ) {
				addColumn(col.name(), col.type());
			}
			return this;
		}
		
		public Builder addColumnAll(Iterable<Column> columns) {
			Objects.requireNonNull(columns, "columns");
			
			for ( Column col: columns ) {
				addColumn(col.name(), col.type());
			}
			
			return this;
		}
		
		public Builder addOrReplaceColumnAll(Column... columns) {
			Objects.requireNonNull(columns, "columns");
			
			for ( Column col: columns ) {
				addOrReplaceColumn(col.name(), col.type());
			}
			return this;
		}
		
		public Builder addOrReplaceColumnAll(Iterable<Column> columns) {
			Objects.requireNonNull(columns, "columns");
			
			for ( Column col: columns ) {
				addOrReplaceColumn(col.name(), col.type());
			}
			return this;
		}
		
		public Builder removeColumn(String colName) {
			Objects.requireNonNull(colName, "column name");
			
			CIString key = CIString.of(colName);
			LinkedHashMap<CIString, Column> old = m_columns;
			m_columns = new LinkedHashMap<>(old.size());
			KVFStream.of(old)
					.filterKey(k -> !k.equals(key))
					.toValueStream()
					.forEach(this::addColumn);
			
			return this;
		}
		
		@Override
		public String toString() {
			return m_columns.values().
							stream()
							.map(Column::toString)
							.collect(Collectors.joining(","));
		}
	}
}
