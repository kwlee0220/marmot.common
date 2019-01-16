package marmot.support;

import java.util.Collection;
import java.util.List;

import com.google.common.collect.Lists;

import utils.CSV;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Functions {
	@MVELFunction(name="ST_LastElement")
	public static String ST_LastElement(String obj, String delim) {
		if ( obj != null ) {
			List<String> list = toList(obj.toString(), delim);
			return list.get(list.size()-1);
		}
		else {
			return null;
		}
	}
	
	@MVELFunction(name="ST_ContainsElement")
	public static boolean ST_ContainsElement(String obj, String delim, String elm) {
		if ( obj != null ) {
			List<String> list = toList(obj.toString(), delim);
			return list.contains(elm);
		}
		else {
			return false;
		}
	}
	
	@MVELFunction(name="ST_StringToCsv")
	public static String ST_StringToCsv(Collection<String> list, String delim) {
		if ( list != null ) {
			return FStream.of(list).join(delim);
		}
		else {
			return null;
		}
	}
	
	@MVELFunction(name="ST_StringFromCsv")
	public static List<String> ST_StringFromCsv(String csv, String delim) {
		if ( csv != null ) {
			return CSV.parseCsv(csv, delim.charAt(0), '\\').toList();
		}
		else {
			return null;
		}
	}
	
	private static List<String> toList(String str, String delim) {
		return (str.length() > 0)
					? Lists.newArrayList(str.split(delim))
					: Lists.newArrayList();
	}
}
