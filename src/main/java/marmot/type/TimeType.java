package marmot.type;

import java.time.LocalTime;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TimeType extends DataType implements ComparableDataType {
	private static final TimeType TYPE = new TimeType();
	
	public static TimeType get() {
		return TYPE;
	}
	
	private TimeType() {
		super("time", TypeCode.TIME, LocalTime.class);
	}

	@Override
	public LocalTime newInstance() {
		return LocalTime.now();
	}
	
	@Override
	public LocalTime parseInstance(String str) {
		str = str.trim();
		return (str.length() > 0) ? LocalTime.parse(str) : null;
	}
}
