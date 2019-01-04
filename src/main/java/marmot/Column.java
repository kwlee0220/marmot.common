package marmot;

import java.util.Objects;

import marmot.proto.ColumnProto;
import marmot.support.PBSerializable;
import marmot.type.DataType;
import marmot.type.DataTypes;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class Column implements PBSerializable<ColumnProto> {
	private final String m_name;
	private final DataType m_type;
	private final short m_ordinal;
	
	public Column(String name, DataType type, int ordinal) {
		Objects.requireNonNull(name, "column name");
		
		m_name = name;
		m_type = type;
		m_ordinal = (short)ordinal;
	}

	public Column(String name, DataType type) {
		Objects.requireNonNull(name, "column name");
		
		m_name = name;
		m_type = type;
		m_ordinal = -1;
	}
	
	/**
	 * 컬럼 이름을 반환한다.
	 * 
	 * @return	컬럼 이름.
	 */
	public String name() {
		return m_name;
	}
	
	/**
	 * 컬럼 타입을 반환한다.
	 * 
	 * @return	컬럼 타입 객체.
	 */
	public DataType type() {
		return m_type;
	}
	
	/**
	 * 컬머의 정의 순번을 반환한다.
	 * 
	 * @return	순번.
	 */
	public int ordinal() {
		return m_ordinal;
	}
	
	public static Column parse(String colStr) {
		String[] parts = colStr.split(":");
		String typeStr = parts[1].trim();
		return new Column(parts[0].trim(),
							typeStr.equals("?") ? null : DataTypes.fromName(typeStr));
	}
	
	@Override
	public String toString() {
		return m_name + ":" + ((m_type != null) ?m_type.getName() : "?");
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		if ( obj == null || !getClass().equals(obj.getClass()) ) {
			return false;
		}
		
		Column other = (Column)obj;
		return m_name.equals(other.m_name)
				&& m_type.getTypeCode() == other.m_type.getTypeCode();
	}
	
	@Override
	public int hashCode() {
		return Objects.hash(m_name, m_type);
	}
	
	public static Column fromProto(ColumnProto proto) {
		String name = proto.getName().toLowerCase();
		DataType type = DataTypes.fromTypeCode((byte)proto.getTypeCodeValue());
		
		return new Column(name, type);
	}

	@Override
	public ColumnProto toProto() {
		return ColumnProto.newBuilder()
							.setName(m_name)
							.setTypeCodeValue(m_type.getTypeCode().ordinal())
							.build();
	}
}