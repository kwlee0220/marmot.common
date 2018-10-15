package marmot.rset;

import java.util.function.Function;

import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.support.DefaultRecord;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TransformedRecordSet extends AbstractRecordSet {
	private final RecordSet m_input;
	private final RecordSchema m_outSchema;
	private final Transform m_transform;
	private final Record m_inputRecord;
	
	@FunctionalInterface
	public interface Transform {
		public void transform(Record input, Record output);
	}
	
	public TransformedRecordSet(RecordSet input, Function<Record,Record> transform) {
		this(input, input.getRecordSchema(), transform);
	}
	
	public TransformedRecordSet(RecordSet input, RecordSchema outSchema,
								Function<Record,Record> transform) {
		this(input, outSchema, (ir,or) -> or.set(transform.apply(ir), true));
	}
	
	public TransformedRecordSet(RecordSet input, RecordSchema outSchema,
								Transform transform) {
		m_input = input;
		m_transform = transform;
		m_outSchema = outSchema;
		m_inputRecord = DefaultRecord.of(input.getRecordSchema());
	}

	@Override
	protected void closeInGuard() {
		m_input.closeQuietly();
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_outSchema;
	}
	
	@Override
	public boolean next(Record record) {
		if ( m_input.next(m_inputRecord) ) {
			m_transform.transform(m_inputRecord, record);
			return true;
		}
		else {
			return false;
		}
	}
}
