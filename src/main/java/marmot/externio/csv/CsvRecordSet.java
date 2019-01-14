package marmot.externio.csv;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.Objects;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vavr.control.Try;
import marmot.Record;
import marmot.RecordSchema;
import marmot.rset.AbstractRecordSet;
import marmot.type.DataType;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CsvRecordSet extends AbstractRecordSet {
	private static final Logger s_logger = LoggerFactory.getLogger(CsvRecordSet.class);
	
	private final CsvParameters m_params;
	private final CSVParser m_parser;
	private final Iterator<CSVRecord> m_iter;
	private final RecordSchema m_schema;
	private CSVRecord m_first;
	
	public static CsvRecordSet from(InputStream is, CsvParameters params) throws IOException {
		BufferedReader reader = new BufferedReader(new InputStreamReader(is, params.charset()));
		return new CsvRecordSet(reader, params);
	}
	
	public static CsvRecordSet from(BufferedReader reader, CsvParameters params) throws IOException {
		return new CsvRecordSet(reader, params);
	}
	
	public static CsvRecordSet from(File file, CsvParameters params) throws IOException {
		return new CsvRecordSet(Files.newBufferedReader(file.toPath(), params.charset()), params);
	}
	
	private CsvRecordSet(BufferedReader reader, CsvParameters params) throws IOException {
		Objects.requireNonNull(reader);
		Objects.requireNonNull(params);
		
		m_params = params;
		m_parser = params.formatForRead().parse(reader);

		m_iter = m_parser.iterator();
		if ( m_parser.getHeaderMap() == null ) {
			m_first = m_iter.next();
			
			int nCols = m_first.size();
			m_schema = FStream.range(0, nCols)
							.map(idx -> String.format("field_%02d", idx))
							.foldLeft(RecordSchema.builder(), (b,n) -> b.addColumn(n, DataType.STRING))
							.build();
		}
		else {
			RecordSchema.Builder builder = RecordSchema.builder();
			for ( String colName: m_parser.getHeaderMap().keySet() ) {
				// marmot에서는 컬럼이름에 '.'이 들어가는 것을 허용하지 않기 때문에
				// '.' 문자를 '_' 문제로 치환시킨다.
				if ( colName.indexOf('.') > 0 )  {
					String replaced = colName.replaceAll("\\.", "_");
					s_logger.warn("column name replaced: '{}' -> '{}'", colName, replaced);
					colName = replaced;
				}
				// marmot에서는 컬럼이름에 공백문자가  들어가는 것을 허용하지 않기 때문에
				// 공백문자를 '_' 문제로 치환시킨다.
				if ( colName.indexOf(' ') > 0 )  {
					String replaced = colName.replaceAll(" ", "_");
					s_logger.warn("column name replaced: '{}' -> '{}'", colName, replaced);
					colName = replaced;
				}
				if ( colName.indexOf('(') > 0 )  {
					String replaced = colName.replaceAll("\\(", "_");
					s_logger.warn("column name replaced: '{}' -> '{}'", colName, replaced);
					colName = replaced;
				}
				if ( colName.indexOf(')') > 0 )  {
					String replaced = colName.replaceAll("\\)", "_");
					s_logger.warn("column name replaced: '{}' -> '{}'", colName, replaced);
					colName = replaced;
				}
				builder.addColumn(colName, DataType.STRING);
			}
			
			m_schema = builder.build();
		}
	}
	
	@Override
	protected void closeInGuard() {
		Try.run(m_parser::close);
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_schema;
	}
	
	@Override
	public boolean next(Record output) {
		checkNotClosed();
		
		if ( m_first != null ) {
			for ( int i =0; i < m_schema.getColumnCount(); ++i ) {
				output.set(i, m_first.get(i));
			}
			
			m_first = null;
			return true;
		}
		
		if ( m_iter.hasNext() ) {
			CSVRecord record = m_iter.next();
			for ( int i =0; i < m_schema.getColumnCount(); ++i ) {
				output.set(i, record.get(i));
			}
			
			return true;
		}
		else {
			return false;
		}
	}
	
	@Override
	public String toString() {
		return String.format("%s[%s]", getClass().getSimpleName(), m_params);
	}
}