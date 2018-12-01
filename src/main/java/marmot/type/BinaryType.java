package marmot.type;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class BinaryType extends DataType {
	private static final BinaryType TYPE = new BinaryType();
	
	public static BinaryType get() {
		return TYPE;
	}
	
	private BinaryType() {
		super("binary", TypeCode.BINARY, byte[].class);
	}

	@Override
	public byte[] newInstance() {
		return new byte[0];
	}
	
	@Override
	public Object fromString(String str) {
		throw new UnsupportedOperationException();
	}
}
