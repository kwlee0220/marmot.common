package marmot.rset;

import java.util.Objects;

import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import net.jcip.annotations.GuardedBy;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class CloserAttachedRecordSet extends AbstractRecordSet {
	private final RecordSet m_rset;
	@GuardedBy("this") private Runnable m_closer;
	
	CloserAttachedRecordSet(RecordSet rset, Runnable closer) {
		Objects.requireNonNull(closer, "closer");
		
		m_rset = rset;
		m_closer = closer;
	}

	@Override
	protected void closeInGuard() {
		m_rset.closeQuietly();
		
		synchronized ( this ) {
			m_closer.run();
		}
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_rset.getRecordSchema();
	}
	
	@Override
	public boolean next(Record record) {
		checkNotClosed();
		
		return m_rset.next(record);
	}
	
	@Override
	public Record nextCopy() {
		checkNotClosed();
		
		return m_rset.nextCopy();
	}
}
