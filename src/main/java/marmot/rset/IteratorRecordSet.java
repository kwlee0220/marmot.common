package marmot;

import java.util.Iterator;
import java.util.Objects;

import marmot.rset.AbstractRecordSet;
import utils.io.IOUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class IteratorRecordSet extends AbstractRecordSet {
	private final RecordSchema m_schema;
	private final Iterator<? extends Record> m_iter;
	
	IteratorRecordSet(RecordSchema schema, Iterator<? extends Record> iter) {
		Objects.requireNonNull(schema, "RecordSchema is null");
		Objects.requireNonNull(iter, "Iterator is null");
		
		m_schema = schema;
		m_iter = iter;
	}
	
	@Override protected void closeInGuard() {
		if ( m_iter instanceof AutoCloseable ) {
			IOUtils.closeQuietly((AutoCloseable)m_iter);
		}
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_schema;
	}
	
	@Override
	public Record nextCopy() {
		checkNotClosed();
		
		if ( m_iter.hasNext() ) {
			return m_iter.next();
		}
		else {
			return null;
		}
	}
}
