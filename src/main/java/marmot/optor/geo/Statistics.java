package marmot.optor.geo;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Statistics {
	public long m_count;
	public double m_avg;
	public double m_stddev;
	public double m_stddev2;
	
	@SuppressWarnings("unused")
	private Statistics() { }
	
	public Statistics(long count, double avg, double stddev) {
		m_count = count;
		m_avg = avg;
		m_stddev = stddev;
	}
}