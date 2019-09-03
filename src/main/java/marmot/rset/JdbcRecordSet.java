package marmot.rset;

import java.sql.ResultSet;
import java.sql.SQLException;

import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSetException;
import utils.Utilities;
import utils.func.Try;
import utils.io.IOUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class JdbcRecordSet extends AbstractRecordSet {
	private final JdbcRecordAdaptor m_adaptor;
	private ResultSet m_rs;
	
	public JdbcRecordSet(JdbcRecordAdaptor adaptor, ResultSet rs) {
		Utilities.checkNotNullArgument(adaptor, "adaptor is null");
		Utilities.checkNotNullArgument(rs, "rs is null");
		
		m_adaptor = adaptor;
		m_rs = rs;
	}
	
	@Override
	protected void closeInGuard() {
		IOUtils.closeQuietly(m_rs);
		m_rs = null;
	}
	
	@Override
	public RecordSchema getRecordSchema() {
		return m_adaptor.getRecordSchema();
	}
	
	@Override
	public boolean next(Record record) throws RecordSetException {
		checkNotClosed();
		
		try {
			if ( m_rs == null ) {
				return false;
			}
			if ( !m_rs.next() ) {
				Try.run(m_rs::close);
				m_rs = null;
				
				return false;
			}
			
			m_adaptor.loadRecord(m_rs, record);
			return true;
		}
		catch ( SQLException e ) {
			throw new RecordSetException(e);
		}
	}
	
	@Override
	public String toString() {
		return String.format("%s", getClass().getName());
	}
}
