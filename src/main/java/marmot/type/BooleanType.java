package marmot.type;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class BooleanType extends DataType {
	private static final BooleanType TYPE = new BooleanType();
	
	public static BooleanType get() {
		return TYPE;
	}
	
	private BooleanType() {
		super("boolean", TypeCode.BOOLEAN, Boolean.class);
	}

	@Override
	public Boolean newInstance() {
		return false;
	}
	
	@Override
	public Boolean parseInstance(String str) {
		str = str.trim();
		return (str.length() > 0) ? Boolean.parseBoolean(str) : null;
	}
}
