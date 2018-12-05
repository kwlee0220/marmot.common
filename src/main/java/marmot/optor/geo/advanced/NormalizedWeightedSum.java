package marmot.optor.geo.advanced;

import java.util.List;

import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class NormalizedWeightedSum extends InterpolationMethod {
	private final WeightFunction m_wfunc;
	
	public static NormalizedWeightedSum with(WeightFunction wfunc) {
		return new NormalizedWeightedSum(wfunc);
	}
	
	public static NormalizedWeightedSum ofParameter(String weightFuncStr) {
		WeightFunction wfunc = WeightFunction.fromString(weightFuncStr);
		return new NormalizedWeightedSum(wfunc);
	}
	
	private NormalizedWeightedSum(WeightFunction wfunc) {
		m_wfunc = wfunc;
	}

	@Override
	public double interpolate(List<SpatialFactor> factors) {
		Interm result = FStream.of(factors)
							.map(factor -> new Interm(factor, m_wfunc))
							.reduce((i1, i2) -> i1.add(i2));
		return result.m_numerator / result.m_denominator;
	}

	@Override
	public String toStringExpr() {
		return String.format("normalized_weighted_sum:%s", m_wfunc.toStringExpr());
	}
	
	private static class Interm {
		private double m_numerator;
		private double m_denominator;
		
		Interm(SpatialFactor factor, WeightFunction wfunc) {
			double weight = wfunc.apply(factor.getDistance());
			m_numerator = weight * factor.getMeasure();
			m_denominator = weight;
		}
		
		Interm(double num, double denom) {
			m_numerator = num;
			m_denominator = denom;
		}
		
		Interm add(Interm other) {
			return new Interm(m_numerator+other.m_numerator,
							m_denominator+other.m_denominator);
		}
	}
}
