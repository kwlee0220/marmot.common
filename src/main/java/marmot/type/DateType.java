package marmot.type;

import java.sql.Date;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DateType extends DataType implements ComparableDataType {
	private static final long serialVersionUID = 1L;
	private static final DateType TYPE = new DateType();
	
	public static DateType get() {
		return TYPE;
	}
	
	private DateType() {
		super("date", TypeCode.DATE, Date.class);
	}

	@Override
	public Date newInstance() {
		return new Date(new java.util.Date().getTime());
	}
	
	@Override
	public Date parseInstance(String str) {
		str = str.trim();
		return (str.length() > 0) ? Date.valueOf(str) : null;
	}
}
