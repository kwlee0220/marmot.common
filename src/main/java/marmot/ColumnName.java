package marmot;

import io.vavr.Lazy;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
final class ColumnName implements Comparable<ColumnName> {
	private final String m_name;
	private final Lazy<String> m_lowerName;
	
	ColumnName(String name) {
		m_name = name;
		m_lowerName = Lazy.of(() -> m_name.toLowerCase());
	}
	
	String get() {
		return m_name;
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
}