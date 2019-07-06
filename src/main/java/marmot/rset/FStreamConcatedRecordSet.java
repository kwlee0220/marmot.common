package marmot.rset;

import marmot.RecordSchema;
import marmot.RecordSet;
import utils.Utilities;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FStreamConcatedRecordSet extends ConcatedRecordSet {
	private final RecordSchema m_schema;
	private final FStream<? extends RecordSet> m_components;
	
	public FStreamConcatedRecordSet(RecordSchema schema,
							FStream<? extends RecordSet> components) {
		Utilities.checkNotNullArgument(schema, "schema is null");
		Utilities.checkNotNullArgument(components, "components is null");
		
		m_schema = schema;
		m_components = components;
	}

	@Override
	protected void closeInGuard() {
		// 남은 RecordSet들을 close 시킨다.
		m_components.forEach(RecordSet::closeQuietly);
		
		super.close();
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_schema;
	}

	@Override
	protected RecordSet loadNext() {
		return m_components.next().getOrNull();
	}
}