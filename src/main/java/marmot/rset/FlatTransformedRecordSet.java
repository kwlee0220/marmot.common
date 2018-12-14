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
public class FlatTransformedRecordSet extends AbstractRecordSet {
	private final RecordSet m_input;
	private final RecordSchema m_outSchema;
	private final Function<Record,RecordSet> m_transform;
	
	private RecordSet m_transformeds;
	private final Record m_inputRecord;
	
	private long m_inCount =0;
	private long m_outCount =0;
	
	public FlatTransformedRecordSet(RecordSet input, RecordSchema outSchema,
									Function<Record,RecordSet> transform) {
		m_input = input;
		m_transform = transform;
		m_outSchema = outSchema;
		
		m_inputRecord = DefaultRecord.of(input.getRecordSchema());
		m_transformeds = RecordSet.empty(outSchema);
	}
	
	@Override
	protected void closeInGuard() {
		m_input.closeQuietly();
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_outSchema;
	}
	
	public long getRecordInputCount() {
		return m_inCount;
	}
	
	public long getRecordOutputCount() {
		return m_outCount;
	}
	
	@Override
	public boolean next(Record record) {
		while ( true ) {
			if ( m_transformeds.next(record) ) {
				++m_outCount;
				
				return true;
			}
			
			if ( !m_input.next(m_inputRecord) ) {
				return false;
			}
			++m_inCount;
			
			m_transformeds = m_transform.apply(m_inputRecord);
		}
	}
}
