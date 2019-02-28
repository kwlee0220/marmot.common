package marmot.type;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class IntervalType extends DataType {
	private static final IntervalType TYPE = new IntervalType();
	
	public static IntervalType get() {
		return TYPE;
	}
	
	private IntervalType() {
		super("interval", TypeCode.INTERVAL, Interval.class);
	}

	@Override
	public Interval newInstance() {
		return Interval.between(0, 0);
	}
	
	@Override
	public Interval parseInstance(String str) {
		throw new UnsupportedOperationException();
	}
}
