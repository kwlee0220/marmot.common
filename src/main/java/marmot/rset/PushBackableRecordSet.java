package marmot.rset;

import java.util.Objects;

import io.vavr.control.Option;
import marmot.Record;
import marmot.RecordSet;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface PushBackableRecordSet extends RecordSet {
	public void pushBack(Record record);
	
	public default boolean hasNext() {
		return peekCopy().isDefined();
	}
	
	public default boolean peek(Record output) {
		Objects.requireNonNull(output);
		
		return nextCopy().peek(this::pushBack).isDefined();
	}
	
	public default Option<Record> peekCopy() {
		return nextCopy().peek(this::pushBack);
	}
}
