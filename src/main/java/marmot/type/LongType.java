package marmot.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


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
	public Long fromString(String str) {
		str = str.trim();
		return (str.length() > 0) ? Long.parseLong(str) : null;
	}

	@Override
	public Long readObject(DataInput in) throws IOException {
		return in.readLong();
	}

	@Override
	public void writeObject(Object obj, DataOutput out) throws IOException {
		out.writeLong((long)obj);
	}
}
