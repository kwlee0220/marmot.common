package marmot.process.geo;

import java.util.function.Function;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface DistanceDecayFunction extends Function<Double,Double> {
	public String toString();
}
