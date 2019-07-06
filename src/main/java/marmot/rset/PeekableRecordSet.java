package marmot.rset;

import org.slf4j.Logger;

import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.RecordSetException;
import marmot.support.ProgressReportable;
import utils.StopWatch;
import utils.Utilities;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PeekableRecordSet extends AbstractRecordSet implements ProgressReportable {
	private final RecordSet m_input;
	private FOption<Record> m_peeked = null;
	
	public PeekableRecordSet(RecordSet input) {
		Utilities.checkNotNullArgument(input, "Peeking RecordSet is null");
		
		m_input = input;
	}
	
	@Override
	protected void closeInGuard() {
		m_input.closeQuietly();
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_input.getRecordSchema();
	}
	
	public boolean hasNext() {
		checkNotClosed();

		if ( m_peeked == null ) {
			m_peeked = FOption.ofNullable(m_input.nextCopy());
		}
		
		return m_peeked.isPresent();
	}
	
	public FOption<Record> peek() {
		checkNotClosed();

		if ( m_peeked == null ) {
			m_peeked = FOption.ofNullable(m_input.nextCopy());
		}
		return m_peeked.map(Record::duplicate);
	}

	@Override
	public boolean next(Record output) throws RecordSetException {
		checkNotClosed();
		
		if ( m_peeked != null ) {
			boolean ret = m_peeked.ifPresent(r -> output.set(r)).isPresent();
			m_peeked = null;
			
			return ret;
		}
		else {
			return m_input.next(output);
		}
	}

	@Override
	public void reportProgress(Logger logger, StopWatch elapsed) {
		if ( m_input instanceof ProgressReportable ) {
			((ProgressReportable)m_input).reportProgress(logger, elapsed);
		}
	}
}
