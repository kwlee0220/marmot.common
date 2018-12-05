package marmot.optor.geo.advanced;

import java.lang.reflect.Method;
import java.util.Map;

import com.google.common.collect.Maps;

import utils.Throwables;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class WeightFunction {
	abstract public double apply(double x);
	abstract public String toStringExpr();
	
	public static final Map<String,Class<? extends WeightFunction>> FUNCTIONS = Maps.newHashMap();
	static {
		FUNCTIONS.put("power", Power.class);
		FUNCTIONS.put("exponential", Exponential.class);
		FUNCTIONS.put("box_cox", BoxCox.class);
		FUNCTIONS.put("inversion", Inversion.class);
	}
	
	public static WeightFunction fromString(String str) {
		int idx = str.indexOf(':');
		if ( idx < 0 ) {
			throw new IllegalArgumentException("invalid interpolation string: " + str);
		}
		String funcName = str.substring(0, idx);
		String paramsStr = str.substring(idx+1);
		
		Class<? extends WeightFunction> funcClass = FUNCTIONS.get(funcName);
		if ( funcClass == null ) {
			throw new IllegalArgumentException("invalid weight-function name: name='"
												+ funcName + "'");
		}
		
		try {
			Method ctor = funcClass.getMethod("ofParameter", String.class);
			Object obj = ctor.invoke(null, paramsStr);
			if ( obj instanceof WeightFunction ) {
				return (WeightFunction)obj;
			}
			else {
				String details = String.format("%s.of(String) does not create "
												+ "an WeightFunction object",
												funcClass.getSimpleName());
				throw new IllegalArgumentException(details);
			}
		}
		catch ( NoSuchMethodException e ) {
			String details = String.format("%s does not provide the static method: %s.ofParameter(String)",
											funcClass.getName(), funcClass.getSimpleName());
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
}
