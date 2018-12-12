package marmot.rset;

import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class AutoClosingRecordSet extends AbstractRecordSet {
	private final RecordSet m_rset;
	
	AutoClosingRecordSet(RecordSet rset) {
		m_rset = rset;
	}

	@Override
	public void closeInGuard() {
		m_rset.close();
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_rset.getRecordSchema();
	}
	
	@Override
	public boolean next(Record record) {
		checkNotClosed();
		
		boolean done = m_rset.next(record);
		if ( !done ) {
			close();
		}
		
		return done;
	}
	
	@Override
	public Record nextCopy() {
		Record next = m_rset.nextCopy();
		if ( next == null ) {
			close();
			return null;
		}
		else {
			return next;
		}
	}
}
