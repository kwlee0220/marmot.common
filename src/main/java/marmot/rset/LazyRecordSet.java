package marmot.rset;

import java.util.function.Supplier;

import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class LazyRecordSet extends AbstractRecordSet {
	private final RecordSchema m_schema;
	private final Supplier<RecordSet> m_supplier;
	private RecordSet m_lazy = null;
	
	LazyRecordSet(RecordSchema schema, Supplier<RecordSet> supplier) {
		m_schema = schema;
		m_supplier = supplier;
	}

	@Override
	protected void closeInGuard() {
		if ( m_lazy != null ) {
			m_lazy.closeQuietly();
		}
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_schema;
	}

	@Override
	public boolean next(Record output) {
		if ( m_lazy == null ) {
			m_lazy = m_supplier.get();
		}
		
		return m_lazy.next(output);
	}

	@Override
	public Record nextCopy() {
		if ( m_lazy == null ) {
			m_lazy = m_supplier.get();
		}
		
		return m_lazy.nextCopy();
	}
}
