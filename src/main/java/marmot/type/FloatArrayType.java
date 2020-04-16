package marmot.type;

import java.util.Comparator;

import utils.CSV;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FloatArrayType extends DataType implements NumericDataType, ContainerDataType,
														Comparator<Float[]> {
	private static final long serialVersionUID = 1L;
	private static final FloatArrayType TYPE = new FloatArrayType();
	
	public static FloatArrayType get() {
		return TYPE;
	}
	
	private FloatArrayType() {
		super("float[]", TypeCode.FLOAT_ARRAY, Float[].class);
	}

	@Override
	public float[] newInstance() {
		return new float[0];
	}
	
	@Override
	public float[] parseInstance(String str) {
		return CSV.parseCsv(str)
					.mapToFloat(fstr -> {
						fstr = fstr.trim();
						return (fstr.length() > 0) ? Float.parseFloat(fstr) : null;
					})
					.toArray();
	}

	@Override
	public DataType getContainerType() {
		return DataType.FLOAT;
	}

	@Override
	public int compare(Float[] v1, Float[] v2) {
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
