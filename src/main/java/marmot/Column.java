package marmot;

import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Objects;

import marmot.proto.ColumnProto;
import marmot.support.PBSerializable;
import marmot.type.DataType;
import marmot.type.DataTypes;
import marmot.type.TypeCode;
import utils.CIString;
import utils.Utilities;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class Column implements PBSerializable<ColumnProto>, Serializable {
	private static final long serialVersionUID = -6927116766846648026L;
	
	private final CIString m_name;
	private final DataType m_type;
	private final short m_ordinal;

	public Column(String name, DataType type) {
		Utilities.checkNotNullArgument(name, "column name is null");
		Utilities.checkNotNullArgument(type, "column type is null");

		m_name = CIString.of(name);
		m_type = type;
		m_ordinal = -1;
	}
	
	public Column(CIString name, DataType type, int ordinal) {
		Utilities.checkNotNullArgument(name, "column name");
		Utilities.checkNotNullArgument(type, "column type");
		
		m_name = name;
		m_type = type;
		m_ordinal = (short)ordinal;
	}
	
	/**
	 * 컬럼 이름을 반환한다.
	 * 
	 * @return	컬럼 이름.
	 */
	public String name() {
		return m_name.get();
	}
	
	public CIString ciName() {
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
		if ( parts.length < 2 ) {
			throw new IllegalArgumentException("invalid column specification: '" + colStr + "'");
		}
		
		String typeStr = parts[1].trim();
		return new Column(parts[0].trim(),
							typeStr.equals("?") ? null : DataTypes.fromName(typeStr));
	}
	
	public String toStringExpr() {
		return m_name.get() + ":" + ((m_type != null) ?m_type.getName() : "?");
	}
	
	public boolean matches(String name) {
		return m_name.matches(name);
	}
	
	@Override
	public String toString() {
		return m_name.get() + ":" + ((m_type != null) ?m_type.getName() : "?");
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
		DataType type = DataTypes.fromTypeCode((byte)proto.getTypeCodeValue());
		
		return new Column(proto.getName(), type);
	}

	@Override
	public ColumnProto toProto() {
		return ColumnProto.newBuilder()
							.setName(m_name.get())
							.setTypeCodeValue(m_type.getTypeCode().get())
							.build();
	}
	
	private Object writeReplace() {
		return new SerializationProxy(this);
	}
	
	private void readObject(ObjectInputStream stream) throws InvalidObjectException {
		throw new InvalidObjectException("Use Serialization Proxy instead.");
	}

	private static class SerializationProxy implements Serializable {
		private static final long serialVersionUID = 4661876959105417945L;
		
		private final String m_name;
		private final TypeCode m_tc;
		private final int m_ordinal;
		
		private SerializationProxy(Column col) {
			m_name = col.name();
			m_tc = col.type().getTypeCode();
			m_ordinal = col.ordinal();
		}
		
		private Object readResolve() {
			return new Column(CIString.of(m_name), DataTypes.fromTypeCode(m_tc), m_ordinal);
		}
	}
}