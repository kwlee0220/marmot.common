package marmot.type;

import java.util.Comparator;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ShortType extends DataType implements IntegralDataType, Comparator<Short> {
	private static final long serialVersionUID = 1L;
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
	public Short parseInstance(String str) {
		str = str.trim();
		return (str.length() > 0) ? Short.parseShort(str) : null;
	}

	@Override
	public int compare(Short v1, Short v2) {
		return Short.compare(v1, v2);
	}
}
