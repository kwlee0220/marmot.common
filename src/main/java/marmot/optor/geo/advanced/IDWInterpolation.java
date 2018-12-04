package marmot.optor.geo.advanced;

import java.util.List;

import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class IDWInterpolation extends InterpolationMethod {
	private final WeightFunction m_wfunc;
	
	public static IDWInterpolation power(double power) {
		return new IDWInterpolation(Power.using(power));
	}
	
	public static IDWInterpolation using(String weightFuncStr) {
		WeightFunction wfunc = WeightFunction.fromString(weightFuncStr);
		return new IDWInterpolation(wfunc);
	}
	
	private IDWInterpolation(WeightFunction wfunc) {
		m_wfunc = wfunc;
	}

	@Override
	public double interpolate(List<SpatialFactor> factors) {
		Interm result = FStream.of(factors)
							.map(this::toIntermediate)
							.reduce((i1, i2) -> i1.add(i2));
		return result.m_numerator / result.m_denominator;
	}

	@Override
	public String toStringExpr() {
		return String.format("idw:%s", m_wfunc.toStringExpr());
	}
	
	private Interm toIntermediate(SpatialFactor factor) {
		double weight = m_wfunc.apply(factor.getDistance()); 
		if ( weight == 0 ) {
			weight = 1;
		}
		weight = 1/weight;
		
		return new Interm(factor.getMeasure() * weight, weight);
	}
	
	private static class Interm {
		private double m_numerator;
		private double m_denominator;
		
		Interm(double numerator, double denominator) {
			m_numerator = numerator;
			m_denominator = denominator;
		}
		
		Interm add(Interm other) {
			return new Interm(m_numerator+other.m_numerator,
							m_denominator+other.m_denominator);
		}
	}
}
