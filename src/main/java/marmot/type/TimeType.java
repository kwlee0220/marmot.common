package marmot.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.LocalTime;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TimeType extends DataType {
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
	public LocalTime fromString(String str) {
		str = str.trim();
		return (str.length() > 0) ? LocalTime.parse(str) : null;
	}

	@Override
	public LocalTime readObject(DataInput in) throws IOException {
		return LocalTime.ofNanoOfDay(in.readLong());
	}

	@Override
	public void writeObject(Object obj, DataOutput out) throws IOException {
		out.writeLong(((LocalTime)obj).toNanoOfDay());
	}
}
