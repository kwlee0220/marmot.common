package marmot.type;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FloatType extends DataType implements NumericDataType {
	private static final FloatType TYPE = new FloatType();
	
	public static FloatType get() {
		return TYPE;
	}
	
	private FloatType() {
		super("float", TypeCode.FLOAT, Float.class);
	}

	@Override
	public Float newInstance() {
		return new Float(0);
	}
	
	@Override
	public Float parseInstance(String str) {
		str = str.trim();
		return (str.length() > 0) ? Float.parseFloat(str) : null;
	}
}
