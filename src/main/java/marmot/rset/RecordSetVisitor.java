package marmot.rset;

import java.util.function.Supplier;

import org.slf4j.Logger;

import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.support.DefaultRecord;
import marmot.support.ProgressReportable;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class RecordSetVisitor implements Supplier<Long>, ProgressReportable {
	private final RecordSet m_input;
	private volatile boolean m_isClosed = false;

	private long m_count;
	protected long m_elapsed;
	private boolean m_finalProgressReported = false;
	
	protected abstract void start(RecordSchema schema);
	protected abstract void visit(Record record);
	protected abstract void finish();
	
	protected RecordSetVisitor(RecordSet rset) {
		m_input = rset;
	}
	
	public long getCount() {
		return m_count;
	}

	@Override
	public Long get() {
		m_count = 0;
		start(m_input.getRecordSchema());
		
		try {
			Record record = DefaultRecord.of(m_input.getRecordSchema());
			while ( m_input.next(record) ) {
				visit(record);
				++m_count;
			}
			
			return m_count;
		}
		finally {
			m_isClosed = true;
			finish();
			
			m_input.closeQuietly();
		}
	}
	
	@Override
	public void reportProgress(Logger logger, StopWatch elapsed) {
		if ( !m_isClosed || !m_finalProgressReported ) {
			if ( m_input instanceof ProgressReportable ) {
				((ProgressReportable)m_input).reportProgress(logger, elapsed);
			}
			
			m_elapsed = elapsed.getElapsedInMillis();
			logger.info("report: [{}]{}", m_isClosed ? "C": "O", toString());
			
			if ( m_isClosed ) {
				m_finalProgressReported = true;
			}
		}
	}
}
