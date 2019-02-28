package marmot.type;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ReservedType extends DataType {
	private static final ReservedType TYPE = new ReservedType();
	
	private ReservedType() {
		super("reserved", TypeCode.RESERVED01, Byte.class);
	}
	
	public static ReservedType get() {
		return TYPE;
	}
	
	@Override
	public Byte newInstance() {
		throw new AssertionError("should not be called");
	}
	
	@Override
	public Byte parseInstance(String str) {
		throw new AssertionError("should not be called");
	}
}
