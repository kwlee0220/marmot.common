package marmot.analysis.module.geo;

import java.util.Map;

import org.mvel2.templates.TemplateRuntime;

import com.google.common.collect.Maps;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DistanceDecayFunctions {
	private DistanceDecayFunctions() {
		throw new AssertionError("Should not be called: class=" + DistanceDecayFunctions.class);
	}
	
	public static DistanceDecayFunction fromString(String expr) {
		String[] parts = expr.split(":");
		switch ( parts[0].toUpperCase() ) {
			case "POWER":
				return new Power(Double.parseDouble(parts[1]));
			default:
				throw new IllegalArgumentException("unknown distance-decay function: " + expr);
		}
	}
	
	private static class Power implements DistanceDecayFunction {
		private final double m_beta;
		
		Power(double beta) {
			m_beta = beta;
		}

		@Override
		public Double apply(Double distance) {
			return Math.pow(distance, m_beta);
		}
		
		@Override
		public String toString() {
			return String.format("%s:%f", "POWER", m_beta);
		}
	}
	
	public static String POWER_DEF(double beta) {
		return String.format("function distance_decay(dist) { Math.pow(dist, %f) }", beta);
	}
	
	public static String EXPONENTIAL(double beta) {
		return String.format("function distance_decay(dist) { Math.exp(dist * %f) }", beta);
	}
	
	public static String BOX_COX(double beta, double lambda) {
		String templt = "function distance_decay(dist) {"
					  + 	"if ( lambda != 0 ) {"
					  +			"Math.exp(@{beta}*((Math.pow(dist,@{lambda})-1)/@{lambda});"
					  + 	"} else {"
					  + 		"Math.pow(dist,@{beta})"
					  + 	"}"
					  + "}";
		Map<String,Object> vars = Maps.newHashMap();
		vars.put("beta", beta);
		vars.put("lambda", lambda);
		return (String)TemplateRuntime.eval(templt,vars);
	}
}
