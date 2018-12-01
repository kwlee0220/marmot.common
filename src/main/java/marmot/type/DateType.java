package marmot.type;

import java.time.LocalDate;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DateType extends DataType {
	private static final DateType TYPE = new DateType();
	
	public static DateType get() {
		return TYPE;
	}
	
	private DateType() {
		super("date", TypeCode.DATE, LocalDate.class);
	}

	@Override
	public LocalDate newInstance() {
		return LocalDate.now();
	}
	
	@Override
	public LocalDate fromString(String str) {
		str = str.trim();
		return (str.length() > 0) ? LocalDate.parse(str) : null;
	}
}
