package marmot.type;

import marmot.support.TypedObject;


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
}
