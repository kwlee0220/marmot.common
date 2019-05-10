package marmot.rset;

import io.reactivex.Observer;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import utils.Utilities;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ProgressReportingRecordSet extends AbstractRecordSet {
	private final RecordSet m_input;
	private final Observer<Long> m_subject;
	private long m_reportInterval = 0;
	private long m_count = 0;
	
	public ProgressReportingRecordSet(RecordSet input, Observer<Long> observer) {
		Utilities.checkNotNullArgument(input, "input is null");
		Utilities.checkNotNullArgument(observer, "observer is null");
		
		m_input = input;
		m_subject = observer;
	}
	
	public ProgressReportingRecordSet(RecordSet input, Observer<Long> observer,
										long reportInterval) {
		this(input, observer);
		
		m_reportInterval = reportInterval;
	}
	
	@Override
	protected void closeInGuard() {
		m_input.closeQuietly();
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_input.getRecordSchema();
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
			if ( m_input.next(record) ) {
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
		Record next = m_input.nextCopy();
		if ( next != null ) {
			if ( m_reportInterval > 0 && (++m_count % m_reportInterval) == 0 ) {
				m_subject.onNext(m_count);
			}
		}
		
		return next;
	}
}
