package marmot.rset;

import java.util.Stack;

import com.google.common.base.Preconditions;

import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.RecordSetException;
import utils.Utilities;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class PushBackableRecordSetImpl extends AbstractRecordSet implements PushBackableRecordSet {
	private final RecordSet m_input;
	private final Stack<Record> m_pushBackeds;
	
	PushBackableRecordSetImpl(RecordSet rset) {
		Utilities.checkNotNullArgument(rset, "rset is null");
		
		m_input = rset;
		m_pushBackeds = new Stack<>();
	}
	
	@Override
	protected void closeInGuard() {
		m_input.closeQuietly();
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_input.getRecordSchema();
	}

	@Override
	public boolean next(Record record) throws RecordSetException {
		checkNotClosed();
		
		if ( !m_pushBackeds.isEmpty() ) {
			record.set(m_pushBackeds.pop());
			return true;
		}
		else {
			return m_input.next(record);
		}
	}
	
	@Override
	public Record nextCopy() {
		checkNotClosed();

		if ( !m_pushBackeds.isEmpty() ) {
			return m_pushBackeds.pop();
		}
		else {
			return m_input.nextCopy();
		}
	}

	@Override
	public void pushBack(Record record) {
		checkNotClosed();
		Preconditions.checkArgument(m_input.getRecordSchema().equals(record.getRecordSchema()),
									"Push-backed record is incompatible");
		
		m_pushBackeds.push(record.duplicate());
	}
	
	@Override
	public String toString() {
		return m_input.toString();
	}
}
