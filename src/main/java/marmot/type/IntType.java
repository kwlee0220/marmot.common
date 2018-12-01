package marmot.type;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class IntType extends DataType {
	private static final IntType TYPE = new IntType();
	
	public static IntType get() {
		return TYPE;
	}
	
	private IntType() {
		super("int", TypeCode.INT, Integer.class);
	}
	
	@Override
	public Integer newInstance() {
		return new Integer(0);
	}
	
	@Override
	public Integer fromString(String str) {
		str = str.trim();
		return (str.length() > 0) ? Integer.parseInt(str) : null;
	}
}
