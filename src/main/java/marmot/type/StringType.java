package marmot.type;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class StringType extends DataType implements ComparableDataType {
	private static final StringType TYPE = new StringType();
	
	public static StringType get() {
		return TYPE;
	}
	
	private StringType() {
		super("string", TypeCode.STRING, String.class);
	}

	@Override
	public String newInstance() {
		return "";
	}
	
	@Override
	public String parseInstance(String str) {
		return str;
	}
}
