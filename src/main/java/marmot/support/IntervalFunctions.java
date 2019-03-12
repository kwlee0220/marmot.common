package marmot.support;

import static marmot.support.DateTimeFunctions.DateTimeToMillis;

import marmot.type.Interval;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class IntervalFunctions {
	@MVELFunction(name="ST_ITFromDateTime")
//	public static Interval fromDateTime(LocalDateTime start, LocalDateTime end) {
	public static Interval ST_ITFromDateTime(Object startObj, Object endObj) {
		long startMillis = DateTimeToMillis(startObj); 
		long endMillis = DateTimeToMillis(endObj);
		
		return Interval.between(startMillis, endMillis);
	}
	
	@MVELFunction(name="ST_ITOverlaps")
//	public static boolean overlaps(Interval int1, Interval int2) {
	public static boolean ST_ITOverlaps(Object obj1, Object obj2) {
		Interval intv1 = asInterval(obj1);
		Interval intv2 = asInterval(obj2);
		
		boolean flag = intv1.overlaps(intv2);
		if ( flag) System.out.println(flag);
		return flag;
	}

	@MVELFunction(name="ST_ITContainsMillis")
	public static boolean contains(Interval obj, long millis) {
		Interval intv = asInterval(obj);
		
		return intv.contains(millis);
	}
	
	public static Interval asInterval(Object obj) {
		if ( obj == null ) {
			return null;
		}
		else if ( obj instanceof Interval ) {
			return (Interval)obj;
		}
		else {
			throw new IllegalArgumentException("Not Interval: obj=" + obj);
		}
	}
}
