package marmot.rset;

import io.vavr.control.Option;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class AutoCloseRecordSet extends AbstractRecordSet {
	private final RecordSet m_rset;
	
	AutoCloseRecordSet(RecordSet rset) {
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
	public Option<Record> nextCopy() {
		checkNotClosed();
		
		return m_rset.nextCopy()
					.orElse(() -> {
						close();
						return Option.none();
					});
	}
}
