package marmot.type;

import utils.CSV;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DoubleArrayType extends DataType implements NumericDataType, ContainerDataType {
	private static final DoubleArrayType TYPE = new DoubleArrayType();
	
	public static DoubleArrayType get() {
		return TYPE;
	}
	
	private DoubleArrayType() {
		super("double[]", TypeCode.DOUBLE_ARRAY, Double[].class);
	}

	@Override
	public double[] newInstance() {
		return new double[0];
	}
	
	@Override
	public double[] parseInstance(String str) {
		return CSV.parseCsv(str)
					.mapToDouble(dstr -> {
						dstr = dstr.trim();
						return (dstr.length() > 0) ? Double.parseDouble(dstr) : null;
					})
					.toArray();
	}

	@Override
	public DataType getContainerType() {
		return DataType.DOUBLE;
	}
}
