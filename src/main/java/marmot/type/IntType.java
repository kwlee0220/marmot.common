package marmot.type;

import java.util.Comparator;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class IntType extends DataType implements IntegralDataType, Comparator<Integer> {
	private static final long serialVersionUID = 1L;
	private static final IntType TYPE = new IntType();
	
	public static IntType get() {
		return TYPE;
	}
	
	private IntType() {
		super("int", TypeCode.INT, Integer.class);
	}
	
	@Override
	public Integer newInstance() {
		return Integer.valueOf(0);
	}
	
	@Override
	public Integer parseInstance(String str) {
		str = str.trim();
		return (str.length() > 0) ? Integer.parseInt(str) : null;
	}

	@Override
	public int compare(Integer v1, Integer v2) {
		return Integer.compare(v1, v2);
	}
}
