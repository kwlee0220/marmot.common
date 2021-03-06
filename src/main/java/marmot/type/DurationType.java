package marmot.type;

import java.time.Duration;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DurationType extends DataType implements ComparableDataType {
	private static final DurationType TYPE = new DurationType();
	
	public static DurationType get() {
		return TYPE;
	}
	
	private DurationType() {
		super("duration", TypeCode.DURATION, Duration.class);
	}

	@Override
	public Duration newInstance() {
		return Duration.ZERO;
	}
	
	@Override
	public Duration parseInstance(String str) {
		throw new UnsupportedOperationException();
	}
}
