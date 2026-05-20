package marmot.rset;

import utils.Preconditions;

import marmot.Record;
import marmot.RecordSet;

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
		Preconditions.checkNotNullArgument(output, "output is null");
		
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
