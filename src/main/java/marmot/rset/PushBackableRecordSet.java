package marmot.rset;

import java.util.Objects;

import marmot.Record;
import marmot.RecordSet;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface PushBackableRecordSet extends RecordSet {
	public void pushBack(Record record);
	
	public default boolean hasNext() {
		return peekCopy() != null;
	}
	
	public default boolean peek(Record output) {
		Objects.requireNonNull(output);
		
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
