package marmot.support;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Range<T extends Comparable<T>> {
	public enum EndPointType {
		OPEN, CLOSED
	}
	
	private EndPointType m_lowerType;
	private T m_lower;
	private EndPointType m_upperType;
	private T m_upper;
	
	public static <T extends Comparable<T>> Range<T> closed(T lower, T upper) {
		return new Range<>(lower, EndPointType.CLOSED, upper, EndPointType.CLOSED);
	}
	
	private Range(T lower, EndPointType lowerType, T upper, EndPointType upperType) {
		m_lower = lower;
		m_lowerType = lowerType;
		m_upper = upper;
		m_upperType = upperType;
	}
	
	public T upperEndpoint() {
		return m_upper;
	}
	
	public T lowerEndpoint() {
		return m_lower;
	}
	
	public boolean contains(T value) {
		int cmp = value.compareTo(m_lower);
		if ( cmp < 0 || (m_lowerType == EndPointType.OPEN && cmp == 0) ) {
			return false;
		}
		
		cmp = value.compareTo(m_upper);
		if ( cmp > 0 || (m_upperType == EndPointType.OPEN && cmp == 0) ) {
			return false;
		}
		
		return true;
	}
	
	@Override
	public String toString() {
		String lowerStr = (m_lowerType == EndPointType.OPEN)
						? String.format("(%s,", m_lower)
						: String.format("[%s,", m_lower);
		String upperStr = (m_upperType == EndPointType.OPEN)
						? String.format("%s)", m_upper)
						: String.format("%s]", m_upper);
						
		return lowerStr + upperStr;
	}
}
