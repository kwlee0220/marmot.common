package marmot.optor.geo.advanced;

import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class EstimateIDWOptions {
	private int m_count = -1;
	private float m_power = -1;
	
	public FOption<Integer> measurementCount() {
		return m_count > 0 ? FOption.of(m_count) : FOption.empty();
	}
	
	public EstimateIDWOptions measurementCount(int count) {
		m_count = count;
		return this;
	}
	
	public FOption<Float> power() {
		return m_power > 0 ? FOption.of(m_power) : FOption.empty();
	}
	
	public EstimateIDWOptions power(float power) {
		m_power = power;
		return this;
	}
}
