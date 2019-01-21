package marmot;

import java.util.Iterator;
import java.util.NoSuchElementException;

import javax.annotation.Nullable;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class RecordSetIterator implements Iterator<Record> {
	private final RecordSet m_rset;
	@Nullable private Record m_next;
	
	RecordSetIterator(RecordSet rset) {
		m_rset = rset;
		m_next = m_rset.nextCopy();
	}
	
	@Override
	public boolean hasNext() {
		return m_next != null;
	}

	@Override
	public Record next() {
		if ( m_next != null ) {
			Record next = m_next;
			if ( (m_next = m_rset.nextCopy()) == null ) {
				m_rset.closeQuietly();
			}
			
			return next;
		}
		else {
			throw new NoSuchElementException();
		}
	}
}