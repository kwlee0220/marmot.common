package marmot.optor.geo.advanced;

import java.util.List;

import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class WeightedSum extends InterpolationMethod {
	private final WeightFunction m_wfunc;
	
	public static WeightedSum with(WeightFunction wfunc) {
		return new WeightedSum(wfunc);
	}
	
	public static WeightedSum ofParameter(String weightFuncStr) {
		WeightFunction wfunc = WeightFunction.fromString(weightFuncStr);
		return new WeightedSum(wfunc);
	}
	
	private WeightedSum(WeightFunction wfunc) {
		m_wfunc = wfunc;
	}

	@Override
	public double interpolate(List<SpatialFactor> factors) {
		if ( factors.size() > 0 ) {
			return FStream.of(factors)
						.mapToDouble(factor -> factor.weighted(m_wfunc))
						.sum();
		}
		else {
			return Double.MIN_VALUE;
		}
	}

	@Override
	public String toStringExpr() {
		return String.format("weighted_sum:%s", m_wfunc.toStringExpr());
	}
}
