package marmot.type;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class LongType extends DataType {
	private static final LongType TYPE = new LongType();
	
	public static LongType get() {
		return TYPE;
	}
	
	private LongType() {
		super("long", TypeCode.LONG, Long.class);
	}

	@Override
	public Long newInstance() {
		return new Long(0);
	}
	
	@Override
	public Long parseInstance(String str) {
		str = str.trim();
		return (str.length() > 0) ? Long.parseLong(str) : null;
	}
}
