package marmot.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FloatType extends DataType {
	private static final FloatType TYPE = new FloatType();
	
	public static FloatType get() {
		return TYPE;
	}
	
	private FloatType() {
		super("float", TypeCode.FLOAT, Float.class);
	}

	@Override
	public Float newInstance() {
		return new Float(0);
	}
	
	@Override
	public Float fromString(String str) {
		str = str.trim();
		return (str.length() > 0) ? Float.parseFloat(str) : null;
	}

	@Override
	public Float readObject(DataInput in) throws IOException {
		return in.readFloat();
	}

	@Override
	public void writeObject(Object obj, DataOutput out) throws IOException {
		out.writeFloat((float)obj);
	}
}
