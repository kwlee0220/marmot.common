package marmot.rset;

import java.util.concurrent.atomic.AtomicBoolean;

import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.RecordSetClosedException;
import marmot.RecordSetException;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class EmptyRecordSet implements RecordSet {
	private final RecordSchema m_schema;
	private AtomicBoolean m_closed = new AtomicBoolean(false);
	
	public EmptyRecordSet(RecordSchema schema) {
		m_schema = schema;
	}

	@Override
	public void close() {
		m_closed.compareAndSet(false, true);
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_schema;
	}

	@Override
	public boolean next(Record record) throws RecordSetException {
		if ( m_closed.get() ) {
			throw new RecordSetClosedException(getClass().getName());
		}
		
		return false;
	}
}