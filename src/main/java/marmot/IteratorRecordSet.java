package marmot;

import java.util.Iterator;
import java.util.Objects;

import marmot.rset.AbstractRecordSet;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class IteratorRecordSet extends AbstractRecordSet {
	private final RecordSchema m_schema;
	private final Iterator<? extends Record> m_iter;
	
	IteratorRecordSet(RecordSchema schema, Iterator<? extends Record> iter) {
		Objects.requireNonNull(schema, "RecordSchema is null");
		Objects.requireNonNull(iter, "Iterator is null");
		
		m_schema = schema;
		m_iter = iter;
	}
	
	@Override protected void closeInGuard() { }

	@Override
	public RecordSchema getRecordSchema() {
		return m_schema;
	}

	@Override
	public boolean next(Record record) throws RecordSetException {
		checkNotClosed();
		
		if ( m_iter.hasNext() ) {
			record.set(m_iter.next(), true);
			return true;
		}
		else {
			return false;
		}
	}
}
