package marmot.rset;

import java.util.Objects;

import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.RecordSetException;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class ConcatedRecordSet extends AbstractRecordSet {
	private RecordSet m_current = null;
	private boolean m_eos = false;
	
	abstract protected RecordSet loadNext();
	
	protected ConcatedRecordSet() {
		m_current = null;
	}
	
	@Override
	protected void closeInGuard() {
		if ( m_current != null ) {
			m_current.closeQuietly();
		}
		
		super.close();
	}

	@Override
	public boolean next(Record record) throws RecordSetException {
		checkNotClosed();
		
		if ( m_eos ) {
			return false;
		}
		
		// first-call?
		if ( m_current == null ) { 
			if ( (m_current = loadNext()) == null ) {
				m_eos = true;
				return false;
			}
		}
		
		while ( !m_current.next(record) ) {
			m_current.closeQuietly();
			 
			if ( (m_current = loadNext()) == null ) {
				m_eos = true;
				return false;
			}
			
			if ( !getRecordSchema().equals(m_current.getRecordSchema()) ) {
				throw new RecordSetException("Component RecordSchema is incompatible to the merged one: "
											+ "concated=" + getRecordSchema()
											+ ", component=" + m_current.getRecordSchema());
			}
		}
		
		return true;
	}
	
	public static ConcatedRecordSet concat(RecordSchema schema, FStream<? extends RecordSet> components) {
		return new FStreamConcatedRecordSet(schema, components);
	}
	private static class FStreamConcatedRecordSet extends ConcatedRecordSet {
		private final RecordSchema m_schema;
		private final FStream<? extends RecordSet> m_components;
		
		private FStreamConcatedRecordSet(RecordSchema schema, FStream<? extends RecordSet> components) {
			Objects.requireNonNull(schema);
			Objects.requireNonNull(components);
			
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
}
