package marmot.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


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
	public Interval fromString(String str) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Interval readObject(DataInput in) throws IOException {
		long start = in.readLong();
		long end = in.readLong();
		
		return Interval.between(start, end);
	}

	@Override
	public void writeObject(Object obj, DataOutput out) throws IOException {
		Interval interval = (Interval)obj;
		
		out.writeLong(interval.getStartMillis());
		out.writeLong(interval.getEndMillis());
	}
}
