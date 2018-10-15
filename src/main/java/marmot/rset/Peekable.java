package marmot.rset;

import io.vavr.control.Option;
import marmot.Record;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface Peekable {
	public Option<Record> peek();
}
