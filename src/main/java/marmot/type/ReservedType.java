package marmot.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ReservedType extends DataType {
	private static final ReservedType TYPE = new ReservedType();
	
	private ReservedType() {
		super("reserved", TypeCode.RESERVED01, Byte.class);
	}
	
	public static ReservedType get() {
		return TYPE;
	}
	
	@Override
	public Byte newInstance() {
		throw new AssertionError("should not be called");
	}
	
	@Override
	public Byte fromString(String str) {
		throw new AssertionError("should not be called");
	}

	@Override
	public Byte readObject(DataInput in) throws IOException {
		throw new AssertionError("should not be called");
	}

	@Override
	public void writeObject(Object obj, DataOutput out) throws IOException {
		throw new AssertionError("should not be called");
	}
}
