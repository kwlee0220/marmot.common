package marmot;

import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class RecordSetStream implements FStream<Record> {
	private final RecordSet m_rset;
	
	RecordSetStream(RecordSet rset) {
		m_rset = rset;
	}

	@Override
	public void close() throws Exception {
		m_rset.closeQuietly();
	}

	@Override
	public Record next() {
		return m_rset.nextCopy();
	}
}