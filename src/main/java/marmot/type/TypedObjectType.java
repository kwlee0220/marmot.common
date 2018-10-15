package marmot.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import marmot.support.TypedObject;
import utils.Utilities;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TypedObjectType extends DataType {
	private static final TypedObjectType TYPE = new TypedObjectType();
	
	public static TypedObjectType get() {
		return TYPE;
	}
	
	public TypedObjectType() {
		super("typed", TypeCode.TYPED, TypedObject.class);
	}

	@Override
	public TypedObject newInstance() {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public Object fromString(String str) {
		throw new UnsupportedOperationException();
	}

	@Override
	public TypedObject readObject(DataInput in) throws IOException {
		try {
			String clsName = in.readUTF();
			Class<?> cls = Class.forName(clsName);
			
			TypedObject typed = (TypedObject)Utilities.callPrivateConstructor(cls);
			typed.readFields(in);
			return typed;
		}
		catch ( Exception e ) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void writeObject(Object obj, DataOutput out) throws IOException {
		TypedObject typed = (TypedObject)obj;
		out.writeUTF(typed.getClass().getName());
		typed.writeFields(out);
	}
}
