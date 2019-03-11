package marmot.rset;

import java.util.function.Predicate;

import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class FilteredRecordSet extends AbstractRecordSet {
	private final RecordSet m_input;
	private final Predicate<Record> m_pred;
	
	public FilteredRecordSet(RecordSet input, Predicate<Record> pred) {
		m_input = input;
		m_pred = pred;
	}

	@Override
	protected void closeInGuard() {
		m_input.closeQuietly();
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_input.getRecordSchema();
	}
	
	@Override
	public boolean next(Record output) {
		while ( next(output) ) {
			if ( m_pred.test(output) ) {
				return true;
			}
		}
		
		return false;
	}
	
	@Override
	public Record nextCopy() {
		Record rec;
		while ( (rec = nextCopy()) != null ) {
			if ( m_pred.test(rec) ) {
				return rec;
			}
		}
		
		return null;
	}
}
