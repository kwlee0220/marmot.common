package marmot.rset;

import io.reactivex.Observer;
import io.reactivex.subjects.BehaviorSubject;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import utils.Utilities;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ProgressReportingRecordSet extends AbstractRecordSet {
	private final RecordSet m_rset;
	private final Observer<Long> m_subject;
	private long m_reportInterval = 0;	// disabled
	private long m_count = 0;
	
	public ProgressReportingRecordSet(RecordSet rset) {
		this(rset, BehaviorSubject.createDefault(0L));
	}
	
	public ProgressReportingRecordSet(RecordSet rset, Observer<Long> observer) {
		Utilities.checkNotNullArgument(rset, "input is null");
		Utilities.checkNotNullArgument(observer, "observer is null");
		
		m_rset = rset;
		m_subject = observer;
	}
	
	@Override
	protected void closeInGuard() {
		m_rset.closeQuietly();
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_rset.getRecordSchema();
	}
	
	public long count() {
		return m_count;
	}
	
	public long reportInterval() {
		return m_reportInterval;
	}
	
	public ProgressReportingRecordSet reportInterval(long intvl) {
		m_reportInterval = intvl;
		return this;
	}
	
	@Override
	public boolean next(Record record) {
		try {
			if ( m_rset.next(record) ) {
				if ( m_reportInterval > 0 && (++m_count % m_reportInterval) == 0 ) {
					m_subject.onNext(m_count);
				}
				
				return true;
			}
			else {
				m_subject.onNext(m_count);
				m_subject.onComplete();
				return false;
			}
		}
		catch ( Exception e ) {
			m_subject.onError(e);
			throw e;
		}
	}
	
	@Override
	public Record nextCopy() {
		Record next = m_rset.nextCopy();
		if ( next != null ) {
			if ( m_reportInterval > 0 && (++m_count % m_reportInterval) == 0 ) {
				m_subject.onNext(m_count);
			}
		}
		
		return next;
	}
}
