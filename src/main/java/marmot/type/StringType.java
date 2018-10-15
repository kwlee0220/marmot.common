package marmot.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class StringType extends DataType {
	private static final StringType TYPE = new StringType();
	
	public static StringType get() {
		return TYPE;
	}
	
	private StringType() {
		super("string", TypeCode.STRING, String.class);
	}

	@Override
	public String newInstance() {
		return "";
	}
	
	@Override
	public String fromString(String str) {
		return str;
	}
	
	@Override
	public String readObject(DataInput in) throws IOException {
		return in.readUTF();
	}

	@Override
	public void writeObject(Object obj, DataOutput out) throws IOException {
		out.writeUTF((String)obj);
	}
}
