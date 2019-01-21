package marmot;

import java.util.Objects;

import io.vavr.control.Try;
import marmot.rset.AbstractRecordSet;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class FStreamRecordSet extends AbstractRecordSet {
	private final RecordSchema m_schema;
	private final FStream<Record> m_stream;
	
	FStreamRecordSet(RecordSchema schema, FStream<Record> stream) {
		Objects.requireNonNull(schema, "RecordSchema is null");
		Objects.requireNonNull(stream, "FStream");
		
		m_schema = schema;
		m_stream = stream;
	}
	
	@Override
	protected void closeInGuard() {
		Try.run(m_stream::close);
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_schema;
	}
	
	@Override
	public Record nextCopy() {
		checkNotClosed();
		
		return m_stream.next().getOrNull();
	}
}
