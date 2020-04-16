package marmot.type;

import java.util.Comparator;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DoubleType extends DataType implements NumericDataType, Comparator<Double> {
	private static final long serialVersionUID = 1L;
	private static final DoubleType TYPE = new DoubleType();
	
	public static DoubleType get() {
		return TYPE;
	}
	
	private DoubleType() {
		super("double", TypeCode.DOUBLE, Double.class);
	}

	@Override
	public Double newInstance() {
		return new Double(0);
	}
	
	@Override
	public Double parseInstance(String str) {
		str = str.trim();
		return (str.length() > 0) ? Double.parseDouble(str) : null;
	}

	@Override
	public int compare(Double v1, Double v2) {
		return Double.compare(v1, v2);
	}
}
