package marmot.support;

import static marmot.support.DateTimeFunctions.ST_DTToMillis;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Date;

import utils.Utilities;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DateFunctions {
	@MVELFunction(name="ST_DateNow")
	public static LocalDate now() {
		return LocalDate.now();
	}

	@MVELFunction(name="ST_DateFromObject")
	public static LocalDate ST_DateFromObject(Object obj) {
		return asLocalDate(obj);
	}

	@MVELFunction(name="ST_DateYear")
	public static int ST_DateYear(Object obj) {
		return asLocalDate(obj).getYear();
	}

	@MVELFunction(name="ST_DateMonthValue")
	public static int ST_DateMonthValue(Object obj) {
		return asLocalDate(obj).getMonthValue();
	}

	@MVELFunction(name="ST_DateDayOfMonth")
	public static int ST_DateDayOfMonth(Object obj) {
		return asLocalDate(obj).getDayOfMonth();
	}

	@MVELFunction(name="ST_DateWeekDay")
	public static int ST_DateWeekDay(Object obj) {
		return asLocalDate(obj).getDayOfWeek().getValue();
	}

	@MVELFunction(name="ST_DateIsEqual")
	public static boolean ST_DateIsEqual(Object left, Object right) {
		return asLocalDate(left).isEqual(asLocalDate(right));
	}

	@MVELFunction(name="ST_DateIsAfter")
	public static boolean ST_DateIsAfter(Object left, Object right) {
		return  asLocalDate(left).isAfter(asLocalDate(right));
	}

	@MVELFunction(name="ST_DateIsBefore")
	public static boolean ST_DateIsBefore(Object left, Object right) {
		return  asLocalDate(left).isBefore(asLocalDate(right));
	}

	@MVELFunction(name="ST_DateFromMillis")
	public static LocalDate ST_DateFromMillis(long millis) {
		return Utilities.fromUTCEpocMillis(millis, ZoneId.systemDefault()).toLocalDate();
	}
	
	@MVELFunction(name="ST_DateToMillis")
	public static long ST_DateToMillis(Object obj) {
		LocalDate date = asLocalDate(obj);
		
		return ST_DTToMillis(date.atStartOfDay());
	}
	
	@MVELFunction(name="ST_DateFromString")
	public static LocalDate ST_DateFromString(String str) {
		return LocalDate.parse(str);
	}
	
	@MVELFunction(name="ST_DateToString")
	public static String ST_DateToString(Object obj) {
		return asLocalDate(obj).toString();
	}

	@MVELFunction(name="ST_DateParse")
	public static LocalDate parse(String dtStr, String pattern) {
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
		return LocalDate.parse(dtStr, formatter);
	}

	@MVELFunction(name="ST_DateParseLE")
	public static LocalDate ST_DateParseLE(String dtStr, DateTimeFormatter formatter) {
		return LocalDate.parse(dtStr, formatter);
	}

	@MVELFunction(name="ST_DatePattern")
	public static DateTimeFormatter ST_DatePattern(String patternStr) {
		return DateTimeFormatter.ofPattern(patternStr);
	}

	@MVELFunction(name="ST_DateFormat")
	public static String ST_DateFormat(Object obj, String pattern) {
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
		return asLocalDate(obj).format(formatter);
	}

	@MVELFunction(name="ST_DateToDateTime")
	public static LocalDateTime ST_DateToDateTime(Object obj) {
		return asLocalDate(obj).atStartOfDay();
	}

	@MVELFunction(name="ST_DateDaysBetween")
	public static long ST_DateDaysBetween(Object date1, Object date2) {
		long gap = ChronoUnit.DAYS.between(asLocalDate(date1), asLocalDate(date2));
		return gap;
	}
	
	public static LocalDate asLocalDate(Object obj) {
		if ( obj == null ) {
			return null;
		}
		if ( obj instanceof LocalDate ) {
			return (LocalDate)obj;
		}
		else if ( obj instanceof Date ) {
			return ((Date)obj).toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
		}
		else if ( obj instanceof LocalDateTime ) {
			return ((LocalDateTime)obj).toLocalDate();
		}
		else {
			throw new IllegalArgumentException("Not Date object: obj=" + obj);
		}
	}
}
