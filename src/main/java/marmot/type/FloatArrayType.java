package marmot.type;

import utils.CSV;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FloatArrayType extends DataType implements NumericDataType, ContainerDataType {
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
}
