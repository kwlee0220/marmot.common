package marmot.support;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;

import utils.Utilities;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DateTimeFunctions {
	@MVELFunction(name="ST_DTNow")
	public static LocalDateTime now() {
		return LocalDateTime.now();
	}

	@MVELFunction(name="ST_DTFromMillis")
	public static LocalDateTime ST_DTFromMillis(long millis) {
		return Utilities.fromUTCEpocMillis(millis, ZoneId.systemDefault());
	}
	
	@MVELFunction(name="ST_DTToMillis")
	public static long ST_DTToMillis(Object obj) {
		return Utilities.toUTCEpocMillis(asDatetime(obj));
	}
	
	@MVELFunction(name="ST_DTFromString")
	public static LocalDateTime ST_DTFromString(String str) {
		return LocalDateTime.parse(str);
	}
	
	@MVELFunction(name="ST_DTToString")
	public static String ST_DTToString(Object obj) {
		return asDatetime(obj).toString();
	}

	@MVELFunction(name="ST_DTGetYear")
	public static int ST_DTGetYear(Object obj) {
		return asDatetime(obj).getYear();
	}

	@MVELFunction(name="ST_DTGetMonth")
	public static int ST_DTGetMonth(Object obj) {
		return asDatetime(obj).getMonthValue();
	}

	@MVELFunction(name="ST_DTGetDayOfMonth")
	public static int ST_DTGetDayOfMonth(Object obj) {
		return asDatetime(obj).getDayOfMonth();
	}

	@MVELFunction(name="ST_DTWeekDay")
	public static int ST_DTWeekDay(Object obj) {
		return asDatetime(obj).getDayOfWeek().getValue();
	}

	@MVELFunction(name="ST_DTGetHour")
	public static int ST_DTGetHour(Object obj) {
		return asDatetime(obj).getHour();
	}

	@MVELFunction(name="ST_DTGetMinute")
	public static int ST_DTGetMinute(Object obj) {
		return asDatetime(obj).getMinute();
	}

	@MVELFunction(name="ST_DTGetSecond")
	public static int ST_DTGetSecond(Object obj) {
		return asDatetime(obj).getSecond();
	}

	@MVELFunction(name="ST_DTParse")
	public static LocalDateTime ST_DTParse(String dtStr, String pattern) {
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
		return LocalDateTime.parse(dtStr, formatter);
	}

	@MVELFunction(name="ST_DTParseLE")
	public static LocalDateTime ST_DTParseLE(String dtStr, DateTimeFormatter formatter) {
		return LocalDateTime.parse(dtStr, formatter);
	}

	@MVELFunction(name="ST_DTPattern")
	public static DateTimeFormatter ST_DTPattern(String patternStr) {
		return DateTimeFormatter.ofPattern(patternStr);
	}

	@MVELFunction(name="ST_DTFormat")
	public static String ST_DTFormat(Object obj, String pattern) {
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
		return asDatetime(obj).format(formatter);
	}

	@MVELFunction(name="ST_DTFormatLE")
	public static String ST_DTFormatLE(Object obj, DateTimeFormatter formatter) {
		return asDatetime(obj).format(formatter);
	}

	@MVELFunction(name="ST_DTIsEqual")
	public static boolean isEqual(LocalDateTime left, LocalDateTime right) {
		return left.isEqual(right);
	}

	@MVELFunction(name="ST_DTIsAfter")
	public static boolean ST_DTIsAfter(Object left, Object right) {
		return asDatetime(left).isAfter(asDatetime(right));
	}

	@MVELFunction(name="ST_DTIsBefore")
	public static boolean isBefore(LocalDateTime left, LocalDateTime right) {
		return left.isBefore(right);
	}

	@MVELFunction(name="ST_DTIsBetween")
	public static boolean ST_DTIsBetween(Object obj, Object begin, Object end) {
		long ldtMillis = ST_DTToMillis(obj);
		long beginMillis = ST_DTToMillis(begin);
		long endMillis = ST_DTToMillis(end);
		
		return ldtMillis >= beginMillis && ldtMillis < endMillis;
	}
	
	public static LocalDateTime asDatetime(Object obj) {
		if ( obj == null ) {
			return null;
		}
		if ( obj instanceof LocalDateTime ) {
			return (LocalDateTime)obj;
		}
		else if ( obj instanceof Date ) {
			return ((Date)obj).toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
		}
		else {
			throw new IllegalArgumentException("Not DateTime object: obj=" + obj);
		}
	}
}
