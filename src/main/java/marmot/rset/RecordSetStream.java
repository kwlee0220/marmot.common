package marmot.rset;

import io.vavr.control.Option;
import marmot.Record;
import marmot.RecordSet;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RecordSetStream implements FStream<Record> {
	private final RecordSet m_rset;
	
	public RecordSetStream(RecordSet rset) {
		m_rset = rset;
	}

	@Override
	public void close() throws Exception {
		m_rset.closeQuietly();
	}

	@Override
	public Option<Record> next() {
		return Option.of(m_rset.nextCopy());
	}
}