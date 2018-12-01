package marmot.type;

import java.time.Duration;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DurationType extends DataType {
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
	public Duration fromString(String str) {
		throw new UnsupportedOperationException();
	}
}
