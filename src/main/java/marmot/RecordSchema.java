package marmot;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import marmot.proto.ColumnProto;
import marmot.proto.RecordSchemaProto;
import marmot.support.PBSerializable;
import marmot.type.DataType;
import marmot.type.DataTypes;
import utils.CSV;
import utils.stream.FStream;
import utils.stream.KVFStream;
import utils.xml.FluentElementImpl;


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
public class RecordSchema implements PBSerializable<RecordSchemaProto>, Serializable  {
	private static final long serialVersionUID = 1L;
	
	/** Empty RecordSchema */
	public static final RecordSchema NULL = builder().build();
	public static final RecordSchema EMPTY = builder().build();
	
	private transient final LinkedHashMap<String,Column> m_columns;
	
	/**
	 * 0개의 컬럼으로 구성된 레코드 스키마를 생성한다.
	 * <p>
	 * 본 생성자는 시스템 내부적으로 사용하는 것으로, 본 생성자를 사용하는 것은 권장되지 않는다.
	 * {@link RecordSchema#builder()}를 통한 레코드 스키마 생성을 권장한다.
	 */
	public RecordSchema() {
		m_columns = Maps.newLinkedHashMap();
	}
	
	private RecordSchema(LinkedHashMap<String,Column> columns) {
		m_columns = FStream.of(columns.values())
							.zipWithIndex()
							.map(t -> new Column(t._1.name().toLowerCase(), t._1.type(), t._2))
							.toKVFStream(Column::name)
							.toMap(Maps.newLinkedHashMap());
	}
	
	/**
	 * 주어진 이름의 컬럼의 존재 여부를 반환한다.
	 * 
	 * @param name	대상 컬럼 이름
	 * @return	이름에 해당하는 컬럼이 존재하는 경우는 true, 그렇지 않은 경우는 false
	 */
	public boolean existsColumn(String name) {
		return m_columns.containsKey(name.toLowerCase());
	}
	
	/**
	 * 컬럼이름에 해당하는 컬럼 정보를 반환한다.
	 * 
	 * @param name	컬럼이름
	 * @return	컬럼 정보 객체
	 * @throws NoSuchElementException	컬럼이름에 해당하는 컬럼이 존재하지 않는 경우
	 */
	public Column getColumn(String name) {
		Column col = m_columns.get(name.toLowerCase());
		if ( col == null ) {
			throw new ColumnNotFoundException("name=" + name + ", schema=" + m_columns.keySet());
		}
		
		return col;
	}
	
	/**
	 * 주어진 이름의 컬럼의 존재 여부를 반환한다.
	 * 만일 해당 이름에 해당하는 컬럼이 존재하지 않는 경우는 인자로 전달된
	 * 기본 컬럼 객체 {@code defValue}를 반환한다.
	 * 
	 * @param name	컬럼이름
	 * @return	컬럼 정보 객체
	 * @param defValue	컬럼이 존재하지 않는 경우 반환될 기본 컬럼 객체
	 */
	public Column getColumn(String name, Column defValue) {
		Objects.requireNonNull(name, "column name is null");
		
		Column col = m_columns.get(name.toLowerCase());
		return col != null ? col : defValue;
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
		return Collections.unmodifiableSet(m_columns.keySet());
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
	
	public FStream<Column> columnFStream() {
		return FStream.of(m_columns.values());
	}
	
	public Stream<Column> columnStream() {
		return m_columns.values().stream();
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
	
	public Stream<Column> stream() {
		return m_columns.values().stream();
	}
	
	public static RecordSchema readFrom(DataInput input) throws IOException {
		RecordSchema.Builder builder = RecordSchema.builder();
		
		short nCols = input.readShort();
		for ( int i =0; i < nCols; ++i ) {
			String name = input.readUTF();
			DataType type = DataTypes.fromTypeCode(input.readByte());
			
			builder.addColumn(name, type);
		}
		
		return builder.build();
	}

	public void writeInto(DataOutput out) throws IOException {
		out.writeShort(m_columns.size());
		for ( Column col: m_columns.values() ) {
			out.writeUTF(col.name());
			out.writeByte((byte)col.type().getTypeCode().ordinal());
		}
	}
	
	public FluentElementImpl toDocumentElement(String docElmName)  {
		return FluentElementImpl.create(docElmName);
	}

	public static RecordSchema fromProto(RecordSchemaProto proto) {
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
		Builder builder = RecordSchema.builder();
		for ( String colSpec: CSV.get().parse(schemaStr.trim()) ) {
			String[] parts = colSpec.split(":");
			if ( parts.length != 2 ) {
				throw new IllegalArgumentException("invalid RecordSchema string: schema='"
													+ schemaStr + "', column='"
													+ colSpec + "'");
			}
			
			builder.addColumn(parts[0].trim(), DataTypes.fromName(parts[1]));
		}
		return builder.build();
	}
	
	@Override
	public String toString() {
		return m_columns.values().
						stream()
						.map(Column::toString)
						.collect(Collectors.joining(","));
	}
	
	public interface IndexedColumnAccessor {
		public void access(int ordinal, Column col);
	}
	
	public Builder toBuilder() {
//		return new Builder(Maps.newLinkedHashMap(m_columns));
		// 별도 다시 생성하여 사용하지 않으면, column 객체의 공유로 인해
		// 문제가 발생할 수 있음.
		Builder builder = builder();
		m_columns.values().stream()
				.forEach(c -> builder.addColumn(c.name(), c.type()));
		return builder;
	}
	
	public static Builder builder() {
		return new Builder();
	}
	
	public static class Builder {
		private LinkedHashMap<String, Column> m_columns;
		
		private Builder() {
			m_columns = Maps.newLinkedHashMap();
		}
		
		private Builder(LinkedHashMap<String,Column> columns) {
			m_columns = columns;
		}
		
		public int size() {
			return m_columns.size();
		}
		
		public RecordSchema build() {
			return new RecordSchema(m_columns);
		}
		
		public boolean existsColumn(String name) {
			Preconditions.checkArgument(name != null, "name should not be null");
			
			return m_columns.containsKey(name);
		}
		
		public Builder addColumn(String name, DataType type) {
			Preconditions.checkArgument(name != null, "name should not be null");
			
			if ( m_columns.putIfAbsent(name, new Column(name, type, m_columns.size())) != null ) {
				throw new IllegalArgumentException("column already exists: name=" + name);
			}
			return this;
		}
		
		public Builder addColumn(Column col) {
			Preconditions.checkArgument(col != null, "Column is null");
			
			return addColumn(col.name(), col.type());
		}
		
		public Builder addColumnIfAbsent(String name, DataType type) {
			Preconditions.checkArgument(name != null, "name should not be null");
			
			m_columns.putIfAbsent(name, new Column(name, type, m_columns.size()));
			return this;
		}
		
		public Builder addOrReplaceColumn(String name, DataType type) {
			Preconditions.checkArgument(name != null, "name should not be null");
			
			name = name.toLowerCase();
			LinkedHashMap<String,Column> columns = Maps.newLinkedHashMap();
			boolean replaced = false;
			for ( Column col: m_columns.values() ) {
				if ( name.equals(col.name()) ) {
					columns.put(name, new Column(name, type));
					replaced = true;
				}
				else {
					columns.put(col.name(), col);
				}
			}
			if ( !replaced ) {
				columns.put(name, new Column(name, type));
			}
			
			m_columns = columns;
			return this;
		}
		
		public Builder addColumnAll(Column... columns) {
			Preconditions.checkArgument(columns != null, "columns should not be null");
			
			for ( Column col: columns ) {
				addColumn(col.name(), col.type());
			}
			return this;
		}
		
		public Builder addColumnAll(Iterable<Column> columns) {
			Preconditions.checkArgument(columns != null, "columns should not be null");
			
			for ( Column col: columns ) {
				addColumn(col.name(), col.type());
			}
			
			return this;
		}
		
		public Builder addOrReplaceColumnAll(Column... columns) {
			Preconditions.checkArgument(columns != null, "columns should not be null");
			
			for ( Column col: columns ) {
				addOrReplaceColumn(col.name(), col.type());
			}
			return this;
		}
		
		public Builder addOrReplaceColumnAll(Iterable<Column> columns) {
			Preconditions.checkArgument(columns != null, "columns should not be null");
			
			for ( Column col: columns ) {
				addOrReplaceColumn(col.name(), col.type());
			}
			return this;
		}
		
		public Builder removeColumn(String colName) {
			Objects.requireNonNull(colName, "column name");
			
			m_columns = KVFStream.of(m_columns)
								.filterKey(k -> !k.equals(colName))
								.toMap(new LinkedHashMap<String,Column>());
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
	
	private Object writeReplace() {
		return new SerializationProxy(this);
	}
	
	private void readObject(ObjectInputStream stream) throws InvalidObjectException {
		throw new InvalidObjectException("Use Serialization Proxy instead.");
	}

	private static class SerializationProxy implements Serializable {
		private static final long serialVersionUID = 3679701773697698351L;
		
		private final RecordSchemaProto m_proto;
		
		private SerializationProxy(RecordSchema mcKey) {
			m_proto = mcKey.toProto();
		}
		
		private Object readResolve() {
			return RecordSchema.fromProto(m_proto);
		}
	}
}
