package marmot.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class IntType extends DataType {
	private static final IntType TYPE = new IntType();
	
	public static IntType get() {
		return TYPE;
	}
	
	private IntType() {
		super("int", TypeCode.INT, Integer.class);
	}
	
	@Override
	public Integer newInstance() {
		return new Integer(0);
	}
	
	@Override
	public Integer fromString(String str) {
		str = str.trim();
		return (str.length() > 0) ? Integer.parseInt(str) : null;
	}

	@Override
	public Integer readObject(DataInput in) throws IOException {
		return in.readInt();
	}

	@Override
	public void writeObject(Object obj, DataOutput out) throws IOException {
		out.writeInt((int)obj);
	}
}
