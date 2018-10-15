package marmot.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class BinaryType extends DataType {
	private static final BinaryType TYPE = new BinaryType();
	
	public static BinaryType get() {
		return TYPE;
	}
	
	private BinaryType() {
		super("binary", TypeCode.BINARY, byte[].class);
	}

	@Override
	public byte[] newInstance() {
		return new byte[0];
	}
	
	@Override
	public Object fromString(String str) {
		throw new UnsupportedOperationException();
	}

	@Override
	public byte[] readObject(DataInput in) throws IOException {
		byte[] bytes = new byte[in.readInt()];
		in.readFully(bytes);
		
		return bytes;
	}

	@Override
	public void writeObject(Object obj, DataOutput out) throws IOException {
		byte[] bytes = (byte[])obj;
		
		out.writeInt(bytes.length);
		out.write(bytes);
	}
}
