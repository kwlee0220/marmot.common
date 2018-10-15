package marmot.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DoubleType extends DataType {
	private static final DoubleType TYPE = new DoubleType();
	
	public static DoubleType get() {
		return TYPE;
	}
	
	private DoubleType() {
		super("double", TypeCode.DOUBLE, Double.class);
	}

	@Override
	public Double newInstance() {
		return new Double(0);
	}
	
	@Override
	public Double fromString(String str) {
		str = str.trim();
		return (str.length() > 0) ? Double.parseDouble(str) : null;
	}

	@Override
	public Double readObject(DataInput in) throws IOException {
		return in.readDouble();
	}

	@Override
	public void writeObject(Object obj, DataOutput out) throws IOException {
		out.writeDouble((double)obj);
	}
}
