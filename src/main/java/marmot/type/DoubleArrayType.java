package marmot.type;

import java.util.Comparator;

import utils.CSV;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DoubleArrayType extends DataType implements NumericDataType, ContainerDataType,
														Comparator<Double[]> {
	private static final long serialVersionUID = 1L;
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

	@Override
	public int compare(Double[] v1, Double[] v2) {
		int length = Math.min(v1.length, v2.length);
		for ( int i =0; i < length; ++i ) {
			int cmp = Double.compare(v1[i], v2[i]);
			if ( cmp != 0 ) {
				return cmp;
			}
		}
		
		return v1.length - v2.length;
	}
}
