package marmot.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneId;

import utils.Utilities;


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

	@Override
	public LocalDate readObject(DataInput in) throws IOException {
		return Utilities.fromUTCEpocMillis(in.readLong(), ZoneId.systemDefault())
						.toLocalDate();
	}

	@Override
	public void writeObject(Object obj, DataOutput out) throws IOException {
		long millis = Utilities.toUTCEpocMillis(((LocalDate)obj).atStartOfDay());
		out.writeLong(millis);
	}
}
