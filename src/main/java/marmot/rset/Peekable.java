package marmot.rset;

import marmot.Record;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface Peekable {
	public FOption<Record> peek();
}
