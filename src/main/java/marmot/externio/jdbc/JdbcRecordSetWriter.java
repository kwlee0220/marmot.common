package marmot.externio.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.Column;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.RecordSetException;
import marmot.externio.RecordSetWriter;
import utils.Utilities;
import utils.jdbc.JdbcProcessor;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class JdbcRecordSetWriter implements RecordSetWriter {
	private final Logger s_logger = LoggerFactory.getLogger(JdbcRecordSetWriter.class);
	private static final int DEFAULT_BATCH_SIZE = 64;
	private static final int DISPLAY_GAP = 100000;
	
	private final String m_tblName;
	private final JdbcProcessor m_jdbc;
	private int m_batchSize = DEFAULT_BATCH_SIZE;
	
	JdbcRecordSetWriter(String tblName, JdbcProcessor jdbc) {
		m_tblName = tblName;
		m_jdbc = jdbc;
	}

	@Override
	public void close() throws IOException {
	}
	
	public JdbcRecordSetWriter batchSize(int size) {
		m_batchSize = size;
		return this;
	}
	
	private String createDefaultInsertValueExpr(RecordSchema schema) {
		String colListExpr = schema.streamColumns().map(Column::name).join(",", "(", ")");
		String colValExpr = FStream.range(0, schema.getColumnCount())
									.map(idx -> "?")
									.join(",", "(", ")");
		return colListExpr + " values " + colValExpr;
	}

	@Override
	public long write(RecordSet rset) throws IOException {
		Utilities.checkNotNullArgument(rset, "rset is null");
		
		String valuesExpr = createDefaultInsertValueExpr(rset.getRecordSchema());
		String insertStmtStr = String.format("insert into %s %s", m_tblName, valuesExpr);
		
		JdbcRecordAdaptor adaptor = JdbcRecordAdaptor.create(m_jdbc, rset.getRecordSchema());
		
		// create table
		try {
			m_jdbc.dropTable(m_tblName);
			adaptor.createTable(m_jdbc, m_tblName);
		}
		catch ( SQLException e1 ) {
			throw new RecordSetException(String.format("fails to store output: jdbc=%s, table=%s",
														m_jdbc, m_tblName), e1);
		}

		AtomicInteger count = new AtomicInteger(0);
		try ( Connection conn = m_jdbc.connect() ) {
			PreparedStatement pstmt = conn.prepareStatement(insertStmtStr);
			s_logger.info("connected: {}", this);
			
			rset.forEachCopy(record -> {
				try {
					adaptor.storeRecord(record, pstmt);
					pstmt.addBatch();
					
					if ( count.incrementAndGet() % m_batchSize == 0 ) {
						pstmt.executeBatch();
						s_logger.debug("inserted: {} records", count);
					}

					if ( count.get() % DISPLAY_GAP == 0 ) {
						s_logger.info("inserted: {} records", count);
					}
				}
				catch ( SQLException e ) {
					throw new RecordSetException(String.format("fails to store output: jdbc=%s, table=%s, cause=%s",
																m_jdbc, m_tblName, e));
				}
			});
			pstmt.executeBatch();
		}
		catch ( Exception e ) {
			System.out.println("count=" + count.get());
			throw new RecordSetException(e);
		}
		s_logger.info("inserted: {} records", count.get());
		
		return count.get();
	}

	@Override
	public String toString() {
		return String.format("%s, tblname=%s", m_jdbc, m_tblName);
	}
}
