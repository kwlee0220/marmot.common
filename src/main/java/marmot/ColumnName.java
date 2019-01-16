package marmot;

import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;

import io.vavr.Lazy;
import utils.stream.FStream;


/**
 * {@code ColumnName}는 Marmot에서 사용하는 컬럼의 이름을 정의한다.
 * <p>
 * 컬럼 이름은 case-insensative하다.
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class ColumnName implements Comparable<ColumnName>, Serializable {
	private static final long serialVersionUID = -2898181438614378345L;
	
	private final String m_name;
	private final Lazy<String> m_lowerName;
	
	public static ColumnName of(String name) {
		return new ColumnName(name);
	}
	
	private ColumnName(String name) {
		Objects.requireNonNull(name, "column name");
		
		m_name = name;
		m_lowerName = Lazy.of(() -> m_name.toLowerCase());
	}
	
	public String get() {
		return m_name;
	}
	
	public static List<ColumnName> fromNameList(List<String> colNames) {
		return FStream.of(colNames).map(ColumnName::of).toList();
	}

	@Override
	public String toString() {
		return m_name;
	}

	@Override
	public int compareTo(ColumnName other) {
		return m_name.compareToIgnoreCase(other.m_name);
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || obj.getClass() != ColumnName.class ) {
			return false;
		}
		
		ColumnName other = (ColumnName)obj;
		return m_name.equalsIgnoreCase(other.m_name);
	}
	
	@Override
	public int hashCode() {
		return m_lowerName.get().hashCode();
	}
	
	private Object writeReplace() {
		return new SerializationProxy(this);
	}
	
	private void readObject(ObjectInputStream stream) throws InvalidObjectException {
		throw new InvalidObjectException("Use Serialization Proxy instead.");
	}

	private static class SerializationProxy implements Serializable {
		private static final long serialVersionUID = -4183428723712485003L;
		
		private transient final String m_strRep;
		
		private SerializationProxy(ColumnName mcKey) {
			m_strRep = mcKey.toString();
		}
		
		private Object readResolve() {
			return ColumnName.of(m_strRep);
		}
	}
}