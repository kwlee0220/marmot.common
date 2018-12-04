package marmot.optor.geo.advanced;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;

import utils.Throwables;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class InterpolationMethod {
	abstract public double interpolate(List<SpatialFactor> factors);
	abstract public String toStringExpr();
	
	public static final Map<String,Class<? extends InterpolationMethod>> METHODS = Maps.newHashMap();
	static {
		METHODS.put("idw", IDWInterpolation.class);
		METHODS.put("weighted_sum", WeightedSum.class);
		METHODS.put("normalized_weighted_sum", NormalizedWeightedSum.class);
		METHODS.put("user_defined", UserDefinedInterpolation.class);
	}
	
	public static InterpolationMethod fromString(String str) {
		int idx = str.indexOf(':');
		if ( idx < 0 ) {
			throw new IllegalArgumentException("invalid interpolation string: " + str);
		}
		String methodName = str.substring(0, idx);
		String methodParam = str.substring(idx+1);
		
		Class<? extends InterpolationMethod> interClass = METHODS.get(methodName);
		if ( interClass == null ) {
			throw new IllegalArgumentException("invalid interpolation name: name='"
												+ methodName + "'");
		}
		
		try {
			Method ctor = interClass.getMethod("using", String.class);
			Object obj = ctor.invoke(null, methodParam);
			if ( obj instanceof InterpolationMethod ) {
				return (InterpolationMethod)obj;
			}
			else {
				String details = String.format("%s.using(String) does not create "
												+ "an InterpolationMethod object",
												interClass.getSimpleName());
				throw new IllegalArgumentException(details);
			}
		}
		catch ( NoSuchMethodException e ) {
			String details = String.format("%s does not provide the static method: %s(String)",
											interClass.getName(), interClass.getSimpleName());
			throw new IllegalArgumentException(details);
		}
		catch ( Exception e ) {
			Throwable cause = Throwables.unwrapThrowable(e);
			throw Throwables.toRuntimeException(cause);
		}
	}
	
	@Override
	public String toString() {
		return toStringExpr();
	}
	
	public static class SpatialFactor {
		private double m_measure;
		private double m_distance;
		
		public SpatialFactor(double measure, double distance) {
			m_measure = measure;
			m_distance = distance;
		}
		
		public double getMeasure() {
			return m_measure;
		}
		
		public double getDistance() {
			return m_distance;
		}
		
		public double weighted(WeightFunction wfunc) {
			return wfunc.apply(m_distance) * m_measure;
		}
		
		@Override
		public String toString() {
			return String.format("(v=%f, dist=%f)", m_measure, m_distance);
		}
	}
}
