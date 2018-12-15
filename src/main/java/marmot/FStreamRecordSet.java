package marmot;

import java.util.Objects;

import io.vavr.control.Try;
import marmot.rset.AbstractRecordSet;
import utils.stream.FStream;
import utils.stream.PrependableFStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class FStreamRecordSet extends AbstractRecordSet {
	private final RecordSchema m_schema;
	private final FStream<Record> m_stream;
	
	FStreamRecordSet(RecordSchema schema, FStream<Record> stream) {
		Objects.requireNonNull(schema, "RecordSchema is null");
		Objects.requireNonNull(stream);
		
		m_schema = schema;
		m_stream = stream;
	}
	
	FStreamRecordSet(FStream<Record> stream) {
		Objects.requireNonNull(stream);
		
		PrependableFStream<Record> prependable = stream.toPrependable();
		m_schema = prependable.peekNext()
							.map(Record::getRecordSchema)
							.getOrElseThrow(()->new RecordSetException("RecordSchema is not known"));
		m_stream = prependable;
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
	public boolean next(Record output) throws RecordSetException {
		checkNotClosed();
		
		return m_stream.next()
						.ifPresent(r -> output.setAll(0, r.getAll()))
						.isPresent();
	}
	
	@Override
	public Record nextCopy() {
		checkNotClosed();
		
		return m_stream.next().getOrNull();
	}
}
