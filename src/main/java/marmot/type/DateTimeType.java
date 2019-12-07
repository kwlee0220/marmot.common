package marmot.type;

import java.time.LocalDateTime;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DateTimeType extends DataType implements ComparableDataType {
	private static final DateTimeType TYPE = new DateTimeType();
	
	public static DateTimeType get() {
		return TYPE;
	}
	
	private DateTimeType() {
		super("datetime", TypeCode.DATETIME, LocalDateTime.class);
	}

	@Override
	public LocalDateTime newInstance() {
		return LocalDateTime.now();
	}
	
	@Override
	public LocalDateTime parseInstance(String str) {
		str = str.trim();
		return (str.length() > 0) ? LocalDateTime.parse(str) : null;
	}
}