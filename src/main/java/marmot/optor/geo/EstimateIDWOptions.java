package marmot.optor.geo;

import io.vavr.control.Option;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class EstimateIDWOptions {
	private int m_count = -1;
	private float m_power = -1;
	
	public Option<Integer> measurementCount() {
		return m_count > 0 ? Option.some(m_count) : Option.none();
	}
	
	public EstimateIDWOptions measurementCount(int count) {
		m_count = count;
		return this;
	}
	
	public Option<Float> power() {
		return m_power > 0 ? Option.some(m_power) : Option.none();
	}
	
	public EstimateIDWOptions power(float power) {
		m_power = power;
		return this;
	}
}
