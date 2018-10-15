package marmot.rset;

import java.util.Iterator;
import java.util.NoSuchElementException;

import io.vavr.control.Option;
import marmot.Record;
import marmot.RecordSet;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RecordSetIterator implements Iterator<Record> {
	private final RecordSet m_rset;
	private Option<Record> m_next;
	
	public RecordSetIterator(RecordSet rset) {
		m_rset = rset;
		m_next = m_rset.nextCopy();
	}
	
	@Override
	public boolean hasNext() {
		return m_next.isDefined();
	}

	@Override
	public Record next() {
		if ( m_next.isDefined() ) {
			Record next = m_next.get();
			m_next = m_rset.nextCopy();
			
			return next;
		}
		else {
			throw new NoSuchElementException();
		}
	}
}