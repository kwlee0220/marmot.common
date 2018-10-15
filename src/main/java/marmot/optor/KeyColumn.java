package marmot.optor;

import java.util.List;
import java.util.Objects;

import marmot.proto.MultiColumnKeyProto.KeyColumnProto;
import marmot.proto.NullsOrderProto;
import marmot.proto.SortOrderProto;
import marmot.support.PBSerializable;
import utils.CSV;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class KeyColumn implements PBSerializable<KeyColumnProto> {
	private final String m_name;
	private final SortOrder m_sortOrder;
	private final NullsOrder m_nullsOrder;
	
	public KeyColumn(String colName) {
		this(colName, SortOrder.NONE, NullsOrder.FIRST);
	}
	
	public KeyColumn(String colName, SortOrder sortOrder, NullsOrder nullsOrder) {
		m_name = colName;
		m_sortOrder = sortOrder;
		m_nullsOrder = nullsOrder;
	}
	
	public String name() {
		return m_name;
	}
	
	public SortOrder sortOrder() {
		return m_sortOrder;
	}
	
	public NullsOrder nullsOrder() {
		return m_nullsOrder;
	}
	
	public KeyColumn duplicate() {
		return new KeyColumn(m_name, m_sortOrder, m_nullsOrder);
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || getClass() != obj.getClass() ) {
			return false;
		}
		
		KeyColumn other = (KeyColumn)obj;
		return Objects.equals(m_name, other.m_name)
			&& Objects.equals(m_sortOrder, other.m_sortOrder)
			&& Objects.equals(m_nullsOrder, other.m_nullsOrder);
	}
	
	@Override
	public int hashCode() {
		return Objects.hash(m_name, m_sortOrder);
	}
	
	public static KeyColumn fromString(String str) {
		SortOrder sorder = SortOrder.NONE;
		NullsOrder norder = NullsOrder.FIRST;
		String colName = null;
		
		List<String> parts = CSV.parse(str, ':', '\\');
		if ( parts.size() == 0 ) {
			throw new IllegalArgumentException("invalid KeyColumn expr='" + str + "'");
		}
		if ( parts.size() == 3 ) {
			sorder = SortOrder.fromString(parts.get(1));
			norder = NullsOrder.fromString(parts.get(2));
		}
		else if ( parts.size() == 2 ) {
			sorder = SortOrder.fromString(parts.get(1));
			norder = (sorder == SortOrder.DESC) ? NullsOrder.FIRST : NullsOrder.LAST; 
		}
		if ( parts.size() >= 1 ) {
			colName = parts.get(0);
		}
		
		return new KeyColumn(colName, sorder, norder);
	}
	
	@Override
	public String toString() {
		if ( m_sortOrder == SortOrder.NONE ) {
			return m_name;
		}
		else {
			return String.format("%s:%s:%s", m_name, m_sortOrder, m_nullsOrder);
		}
	}

	public static KeyColumn fromProto(KeyColumnProto proto) {
		SortOrder sortOrder = SortOrder.valueOf(proto.getSortOrder().name());
		NullsOrder nullsOrder = NullsOrder.valueOf(proto.getNullsOrder().name());
		return new KeyColumn(proto.getColumn(), sortOrder, nullsOrder);
	}

	@Override
	public KeyColumnProto toProto() {
		return KeyColumnProto.newBuilder()
						.setColumn(m_name)
						.setSortOrder(SortOrderProto.valueOf(m_sortOrder.name()))
						.setNullsOrder(NullsOrderProto.valueOf(m_nullsOrder.name()))
						.build();
	}
}