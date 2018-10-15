package marmot.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
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

	@Override
	public Duration readObject(DataInput in) throws IOException {
		long secs = in.readLong();
		int nanos = in.readInt();
		
		return Duration.ofSeconds(secs, nanos);
	}

	@Override
	public void writeObject(Object obj, DataOutput out) throws IOException {
		Duration dur = (Duration)obj;
		
		out.writeLong(dur.getSeconds());
		out.writeInt(dur.getNano());
	}
}
