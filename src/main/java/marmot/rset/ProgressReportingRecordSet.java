package marmot.rset;

import java.util.Objects;

import io.reactivex.Observer;
import io.vavr.control.Option;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;


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
		Objects.requireNonNull(input);
		Objects.requireNonNull(observer);
		
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
	public Option<Record> nextCopy() {
		return m_input.nextCopy()
					.peek(rec -> {
						if ( m_reportInterval > 0 && (++m_count % m_reportInterval) == 0 ) {
							m_subject.onNext(m_count);
						}
					});
	}
}
