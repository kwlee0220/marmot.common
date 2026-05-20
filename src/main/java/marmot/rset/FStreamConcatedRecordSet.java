package marmot.rset;

import utils.Preconditions;
import utils.stream.FStream;

import marmot.RecordSchema;
import marmot.RecordSet;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FStreamConcatedRecordSet extends ConcatedRecordSet {
	private final RecordSchema m_schema;
	private final FStream<? extends RecordSet> m_components;
	
	public FStreamConcatedRecordSet(RecordSchema schema,
							FStream<? extends RecordSet> components) {
		Preconditions.checkNotNullArgument(schema, "schema is null");
		Preconditions.checkNotNullArgument(components, "components is null");
		
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