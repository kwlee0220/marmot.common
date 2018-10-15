package marmot.support;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TimeFunctions {
	@MVELFunction(name="ST_TimeNow")
	public static LocalTime now() {
		return LocalTime.now();
	}

	@MVELFunction(name="ST_TimeHour")
	public static int getHour(Object obj) {
		return asTime(obj).getHour();
	}

	@MVELFunction(name="ST_TimeMinute")
	public static int getMinute(Object obj) {
		return asTime(obj).getMinute();
	}

	@MVELFunction(name="ST_TimeSecond")
	public static int getSecond(Object obj) {
		return asTime(obj).getSecond();
	}

	@MVELFunction(name="ST_TimeIsEqual")
	public static boolean isEqual(Object left, Object right) {
		return asTime(left).equals(asTime(right));
	}

	@MVELFunction(name="ST_TimeIsAfter")
	public static boolean isAfter(Object left, Object right) {
		return asTime(left).isAfter(asTime(right));
	}

	@MVELFunction(name="ST_TimeIsBefore")
	public static boolean isBefore(Object left, Object right) {
		return asTime(left).isBefore(asTime(right));
	}
	
	@MVELFunction(name="ST_TimeFromString")
	public static LocalTime ST_TimeFromString(String str) {
		return LocalTime.parse(str);
	}
	
	@MVELFunction(name="ST_TimeToString")
	public static String ST_TimeToString(Object obj) {
		LocalTime date = asTime(obj);
		
		return date.toString();
	}

	@MVELFunction(name="ST_TimeParse")
	public static LocalTime parse(String dtStr, String pattern) {
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
		return LocalTime.parse(dtStr, formatter);
	}

	@MVELFunction(name="ST_TimeToFormat")
	public static String toFormattedString(LocalTime date, String pattern) {
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
		return date.format(formatter);
	}

	@MVELFunction(name="ST_TimeToDateTime")
	public static LocalDateTime atStartOfDay(LocalDate date, LocalTime time) {
		return time.atDate(date);
	}
	
	public static LocalTime asTime(Object obj) {
		if ( obj != null && obj instanceof LocalTime ) {
			return (LocalTime)obj;
		}
		else {
			throw new IllegalArgumentException("Not asTime: obj=" + obj);
		}
	}
}
