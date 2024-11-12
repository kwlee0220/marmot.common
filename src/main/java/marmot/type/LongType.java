package marmot.type;

import java.util.Comparator;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class LongType extends DataType implements IntegralDataType, Comparator<Long> {
	private static final long serialVersionUID = 1L;
	private static final LongType TYPE = new LongType();
	
	public static LongType get() {
		return TYPE;
	}
	
	private LongType() {
		super("long", TypeCode.LONG, Long.class);
	}

	@Override
	public Long newInstance() {
		return Long.valueOf(0);
	}
	
	@Override
	public Long parseInstance(String str) {
		str = str.trim();
		return (str.length() > 0) ? Long.parseLong(str) : null;
	}

	@Override
	public int compare(Long v1, Long v2) {
		return Long.compare(v1, v2);
	}
}
