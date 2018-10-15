package marmot.rset;

import io.vavr.control.Option;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import net.jcip.annotations.GuardedBy;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CloserAttachedRecordSet extends AbstractRecordSet {
	private final RecordSet m_rset;
	@GuardedBy("this") private Option<Runnable> m_closer;
	
	public CloserAttachedRecordSet(RecordSet rset, Runnable closer) {
		m_rset = rset;
		m_closer = Option.of(closer);
	}
	
	public CloserAttachedRecordSet(RecordSet rset) {
		m_rset = rset;
		m_closer = Option.none();
	}

	@Override
	protected void closeInGuard() {
		m_rset.closeQuietly();
		synchronized ( this ) {
			m_closer.forEach(Runnable::run);
			m_closer = null;
		}
	}
	
	public void setCloser(Runnable closer) {
		m_closer = Option.of(closer);
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
	public Option<Record> nextCopy() {
		checkNotClosed();
		
		return m_rset.nextCopy();
	}
}
