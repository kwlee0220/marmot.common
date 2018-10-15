package marmot.optor;

import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;

import marmot.Column;
import marmot.RecordSchema;
import marmot.proto.MultiColumnKeyProto;
import marmot.support.PBSerializable;
import utils.CSV;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class MultiColumnKey implements PBSerializable<MultiColumnKeyProto>, Serializable {
	private static final long serialVersionUID = 1L;
	public static final MultiColumnKey EMPTY = new MultiColumnKey();
	
	private transient final List<KeyColumn> m_keyColumns;
	
	public static MultiColumnKey of(String colName, SortOrder sortOrder,
									NullsOrder nullsOrder) {
		return new MultiColumnKey(Lists.newArrayList(new KeyColumn(colName.toLowerCase(),
																	sortOrder, nullsOrder)));
	}
	
	public static MultiColumnKey of(String colName, SortOrder sortOrder) {
		switch ( sortOrder ) {
			case DESC:
			case NONE:
				return of(colName, sortOrder, NullsOrder.FIRST);
			case ASC:
				return of(colName, sortOrder, NullsOrder.LAST);
			default:
				throw new AssertionError();
		}
	}
	
	public MultiColumnKey() {
		m_keyColumns = Lists.newArrayList();
	}

	public MultiColumnKey(List<KeyColumn> keyColumns) {
		m_keyColumns = keyColumns;
	}
	
	public int length() {
		return m_keyColumns.size();
	}
	
	public List<KeyColumn> getKeyColumnAll() {
		return m_keyColumns;
	}
	
	public KeyColumn getKeyColumnAt(int idx) {
		return m_keyColumns.get(idx);
	}
	
	public KeyColumn getKeyColumn(String colName) {
		return m_keyColumns.stream()
							.filter(col -> col.name().equals(colName))
							.findFirst().orElse(null);
	}
	
	/**
	 * 주어진 이름의 컬럼의 존재 여부를 반환한다.
	 * 
	 * @param colName	컬럼 이름
	 * @return 키 존재 여부.
	 */
	public boolean existsKeyColumn(String colName) {
		return m_keyColumns.stream()
							.anyMatch(col -> col.name().equals(colName));
	}
	
	public void validate(RecordSchema schema) {
		for ( KeyColumn key: m_keyColumns ) {
			if ( schema.getColumn(key.name(), null) == null ) {
				throw new IllegalArgumentException("input RecordSchema does not have key column: "
													+ key.name());
			}
		}
	}
	
	public static MultiColumnKey complement(RecordSchema schema, MultiColumnKey keyCols) {
		List<String> keyColNames = FStream.of(keyCols.m_keyColumns)
											.map(KeyColumn::name)
											.toList();
		List<KeyColumn> compKeyCols = schema.columnFStream()
											.map(Column::name)
											.filter(name -> !keyColNames.contains(name))
											.map(KeyColumn::new)
											.toList();
		return new MultiColumnKey(compKeyCols);
	}
	
	public MultiColumnKey duplicate() {
		return new MultiColumnKey(m_keyColumns.stream()
											.map(KeyColumn::duplicate)
											.collect(Collectors.toList()));
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || getClass() != obj.getClass() ) {
			return false;
		}
		
		MultiColumnKey other = (MultiColumnKey)obj;
		return m_keyColumns.equals(other.m_keyColumns);
	}
	
	@Override
	public int hashCode() {
		return m_keyColumns.hashCode();
	}
	
	public static MultiColumnKey fromString(String colSpecList) {
		CSV csv = CSV.get().withDelimiter(',').withEscape('\\');
		List<KeyColumn> keyCols = csv.parse(colSpecList).stream()
											.map(String::trim)
											.map(String::toLowerCase)
											.map(KeyColumn::fromString)
											.collect(Collectors.toList());
		return new MultiColumnKey(keyCols);
	}
	
	@Override
	public String toString() {
		return m_keyColumns.stream()
						.map(KeyColumn::toString)
						.collect(Collectors.joining(","));
	}
	
	public static MultiColumnKey concat(MultiColumnKey... keys) {
		List<KeyColumn> keyCols = FStream
				.of(keys)
				.map(key -> key.getKeyColumnAll())
				.collectLeft(Lists.newArrayList(), (a,ks)->a.addAll(ks));
		
		return new MultiColumnKey(keyCols);
	}
	
	public static MultiColumnKey intersection(MultiColumnKey keyCols1, MultiColumnKey keyCols2) {
		List<KeyColumn> sameCols = FStream.of(keyCols1.getKeyColumnAll())
										.map(KeyColumn::name)
										.filter(keyCols2::existsKeyColumn)
										.map(KeyColumn::new)
										.toList();
		return new MultiColumnKey(sameCols);
	}

	public static MultiColumnKey fromProto(MultiColumnKeyProto proto) {
		return new MultiColumnKey(FStream.of(proto.getColumnList())
													.map(KeyColumn::fromProto)
													.toList());
	}

	@Override
	public MultiColumnKeyProto toProto() {
		return FStream.of(m_keyColumns)
						.map(KeyColumn::toProto)
						.foldLeft(MultiColumnKeyProto.newBuilder(),
									(builder,key)->builder.addColumn(key))
						.build();
	}
	
	private Object writeReplace() {
		return new SerializationProxy(this);
	}
	
	private void readObject(ObjectInputStream stream) throws InvalidObjectException {
		throw new InvalidObjectException("Use Serialization Proxy instead.");
	}

	private static class SerializationProxy implements Serializable {
		private static final long serialVersionUID = -6864353473263853643L;
		
		private final MultiColumnKeyProto m_proto;
		
		private SerializationProxy(MultiColumnKey mcKey) {
			m_proto = mcKey.toProto();
		}
		
		private Object readResolve() {
			return MultiColumnKey.fromProto(m_proto);
		}
	}
}
