package marmot.type;

import java.util.Comparator;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FloatType extends DataType implements NumericDataType, Comparator<Float> {
	private static final long serialVersionUID = 1L;
	private static final FloatType TYPE = new FloatType();
	
	public static FloatType get() {
		return TYPE;
	}
	
	private FloatType() {
		super("float", TypeCode.FLOAT, Float.class);
	}

	@Override
	public Float newInstance() {
		return Float.valueOf(0);
	}
	
	@Override
	public Float parseInstance(String str) {
		str = str.trim();
		return (str.length() > 0) ? Float.parseFloat(str) : null;
	}

	@Override
	public int compare(Float v1, Float v2) {
		return Float.compare(v1, v2);
	}
}
