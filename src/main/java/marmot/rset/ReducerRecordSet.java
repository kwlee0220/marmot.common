package marmot.rset;

import org.slf4j.Logger;

import marmot.Record;
import marmot.RecordSet;
import marmot.support.ProgressReportable;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class ReducerRecordSet extends AbstractRecordSet implements ProgressReportable {
	private final RecordSet m_input;
	
	private Record m_result;
	protected long m_elapsed;
	private boolean m_finalProgressReported = false;
	
	protected abstract Record reduce();
	
	protected ReducerRecordSet(RecordSet input) {
		m_input = input;
	}

	@Override
	protected void closeInGuard() throws Exception {
		m_input.close();
	}
	
	@Override
	public Record nextCopy() {
		if ( m_result != null ) {
			return null;
		}
		else {
			return m_result = reduce();
		}
	}

	@Override
	public void reportProgress(Logger logger, StopWatch elapsed) {
		if ( !isClosed() || !m_finalProgressReported ) {
			if ( m_input instanceof ProgressReportable ) {
				((ProgressReportable)m_input).reportProgress(logger, elapsed);
			}
			
			m_elapsed = elapsed.getElapsedInMillis();
			logger.info("report: [{}]{}", isClosed() ? "C": "O", toString());
			
			if ( isClosed() ) {
				m_finalProgressReported = true;
			}
		}
	}
}
