package marmot.externio.csv;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.univocity.parsers.csv.CsvFormat;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

import io.vavr.control.Try;
import marmot.Column;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSetException;
import marmot.rset.AbstractRecordSet;
import marmot.support.DataUtils;
import marmot.type.DataType;
import utils.Utilities;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CsvRecordSet extends AbstractRecordSet {
	private static final Logger s_logger = LoggerFactory.getLogger(CsvRecordSet.class);
	
	private final CsvParameters m_params;
	private final BufferedReader m_reader;
	private final CsvParser m_parser;
	private final RecordSchema m_schema;
	private final Column[] m_columns;
	private String[] m_first;
	
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
		Utilities.checkNotNullArgument(reader, "reader is null");
		Utilities.checkNotNullArgument(params, "params is null");

		m_reader = reader;
		m_params = params;

		CsvParserSettings settings = new CsvParserSettings();
		CsvFormat format = settings.getFormat();
		format.setDelimiter(params.delimiter());
		params.quote().ifPresent(format::setQuote);
		params.escape().ifPresent(format::setCharToEscapeQuoteEscaping);
		params.nullValue().ifPresent(settings::setNullValue);
		m_parser = new CsvParser(settings);
		
		String line = reader.readLine();
		if ( line == null ) {
			throw new IllegalArgumentException("input CSV file is empty");
		}
		m_first = m_parser.parseLine(line);
		
		if ( params.headerFirst() ) {
			m_schema = CsvUtils.buildRecordSchema(m_first);
			m_first = null;
		}
		else if ( params.headerRecord().isPresent() ) {
			String header = params.headerRecord().getUnchecked();
			m_schema = CsvUtils.buildRecordSchema(m_parser.parseLine(header));
		}
		else {
			String[] header = FStream.range(0, m_first.length)
									.map(idx -> String.format("field_%02d", idx))
									.toArray(String.class);
			m_schema = CsvUtils.buildRecordSchema(header);
		}
		m_columns = m_schema.getColumns().toArray(new Column[0]);
	}
	
	@Override
	protected void closeInGuard() {
		Try.run(m_reader::close);
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_schema;
	}
	
	@Override
	public boolean next(Record output) {
		checkNotClosed();
		
		if ( m_first != null ) {
			set(output, m_first);
			m_first = null;
			
			return true;
		}
		
		try {
			String line = m_reader.readLine();
			if ( line != null ) {
				String[] values = m_parser.parseLine(line);
				if ( values.length != m_columns.length ) {
					throw new IOException("invalid CSV line: '" + values + "'");
				}
				set(output, values);
				
				return true;
			}
			else {
				return false;
			}
		}
		catch ( IOException e ) {
			throw new RecordSetException("" + e);
		}
	}
	
	@Override
	public String toString() {
		return String.format("%s[%s]", getClass().getSimpleName(), m_params);
	}
	
	private void set(Record output, String[] values) {
		
		for ( int i =0; i < values.length; ++i ) {
			if ( m_params.trimField() ) {
				values[i] = values[i].trim();
			}
			if ( m_columns[i].type() != DataType.STRING ) {
				output.set(i, DataUtils.cast(values[i], m_columns[i].type()));
			}
			else {
				output.set(i, values[i]);
			}
		}
	}
}