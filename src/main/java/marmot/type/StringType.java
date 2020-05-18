package marmot.type;

import java.util.Comparator;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class StringType extends DataType implements ComparableDataType, Comparator<String> {
	private static final long serialVersionUID = 1L;
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

	@Override
	public int compare(String v1, String v2) {
		if ( v1 != null && v2 != null ) {
			return v1.compareTo(v2);
		}
		else if ( v1 != null ) {
			return 1;
		}
		else if ( v2 != null ) {
			return -1;
		}
		else {
			return 1;
		}
	}
}
