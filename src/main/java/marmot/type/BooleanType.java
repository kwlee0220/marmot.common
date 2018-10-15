package marmot.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class BooleanType extends DataType {
	private static final BooleanType TYPE = new BooleanType();
	
	public static BooleanType get() {
		return TYPE;
	}
	
	private BooleanType() {
		super("boolean", TypeCode.BOOLEAN, Boolean.class);
	}

	@Override
	public Boolean newInstance() {
		return false;
	}
	
	@Override
	public Boolean fromString(String str) {
		str = str.trim();
		return (str.length() > 0) ? Boolean.parseBoolean(str) : null;
	}

	@Override
	public Boolean readObject(DataInput in) throws IOException {
		return in.readBoolean();
	}

	@Override
	public void writeObject(Object obj, DataOutput out) throws IOException {
		out.writeBoolean((boolean)obj);
	}
}
