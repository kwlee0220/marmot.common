package marmot.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ByteType extends DataType {
	private static final ByteType TYPE = new ByteType();
	
	public static ByteType get() {
		return TYPE;
	}
	
	private ByteType() {
		super("byte", TypeCode.BYTE, Byte.class);
	}
	
	@Override
	public Byte newInstance() {
		return new Byte((byte)0);
	}
	
	@Override
	public Byte fromString(String str) {
		str = str.trim();
		return (str.length() > 0) ? Byte.parseByte(str) : null;
	}

	@Override
	public Byte readObject(DataInput in) throws IOException {
		return in.readByte();
	}

	@Override
	public void writeObject(Object obj, DataOutput out) throws IOException {
		out.writeByte((byte)obj);
	}
}
