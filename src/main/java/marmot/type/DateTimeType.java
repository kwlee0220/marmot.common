package marmot.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;

import utils.Utilities;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DateTimeType extends DataType {
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
	public LocalDateTime fromString(String str) {
		str = str.trim();
		return (str.length() > 0) ? LocalDateTime.parse(str) : null;
	}

	@Override
	public LocalDateTime readObject(DataInput in) throws IOException {
		return Utilities.fromUTCEpocMillis(in.readLong(), ZoneId.systemDefault());
	}

	@Override
	public void writeObject(Object obj, DataOutput out) throws IOException {
		out.writeLong(Utilities.toUTCEpocMillis((LocalDateTime)obj));
	}
}