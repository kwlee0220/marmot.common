package marmot.rset;

import marmot.Record;
import marmot.RecordSet;
import utils.Utilities;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface PushBackableRecordSet extends RecordSet {
	public PushBackableRecordSet pushBack(Record record);
	
	public default boolean hasNext() {
		return peekCopy() != null;
	}
	
	public default boolean peek(Record output) {
		Utilities.checkNotNullArgument(output, "output is null");
		
		Record next = nextCopy();
		if ( next != null ) {
			pushBack(next);
		}
		
		return next != null;
	}
	
	public default Record peekCopy() {
		Record next = nextCopy();
		if ( next != null ) {
			pushBack(next);
		}
		
		return next;
	}
}
