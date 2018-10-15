package marmot.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ShortType extends DataType {
	private static final ShortType TYPE = new ShortType();
	
	public static ShortType get() {
		return TYPE;
	}
	
	private ShortType() {
		super("short", TypeCode.SHORT, Short.class);
	}
	
	@Override
	public Short newInstance() {
		return new Short((short)0);
	}
	
	@Override
	public Short fromString(String str) {
		str = str.trim();
		return (str.length() > 0) ? Short.parseShort(str) : null;
	}

	@Override
	public Short readObject(DataInput in) throws IOException {
		return in.readShort();
	}

	@Override
	public void writeObject(Object obj, DataOutput out) throws IOException {
		out.writeShort((short)obj);
	}
}
