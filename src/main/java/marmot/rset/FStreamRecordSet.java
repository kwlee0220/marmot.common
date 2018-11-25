package marmot.rset;

import java.util.Objects;

import io.vavr.control.Option;
import io.vavr.control.Try;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSetException;
import utils.stream.FStream;
import utils.stream.PeekableFStream;

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
		
		PeekableFStream<Record> peekable = stream.toPeekable();
		Record first = peekable.peekNext();
		if ( first == null ) {
			throw new RecordSetException("RecordSchema is not specified");
		}
		m_schema = first.getSchema();
		m_stream = peekable;
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
		
		Record rec = m_stream.next();
		if ( rec != null ) {
			output.setAll(0, rec.getAll());
			return true;
		}
		else {
			return false;
		}
	}
	
	@Override
	public Option<Record> nextCopy() {
		return Option.of(m_stream.next());
	}
}
