package marmot.externio.csv;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import marmot.Column;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSetException;
import marmot.rset.AbstractRecordSet;
import marmot.support.DataUtils;
import marmot.type.DataType;
import utils.Utilities;
import utils.func.Try;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CsvRecordSet extends AbstractRecordSet {
	private static final Logger s_logger = LoggerFactory.getLogger(CsvRecordSet.class);
	
	private final String m_key;
	private final CsvParameters m_options;
	private final CSVParser m_parser;
	private final Iterator<CSVRecord> m_iter;
	private final String m_nullValue;
	private final RecordSchema m_schema;
	private final Column[] m_columns;
	private List<String> m_first;
	private long m_lineNo = 0;
	
	static CsvRecordSet from(String key, InputStream is, CsvParameters opts) throws IOException {
		Utilities.checkNotNullArgument(is, "is is null");
		Utilities.checkNotNullArgument(opts, "CsvOptions is null");
		
		BufferedReader reader = new BufferedReader(new InputStreamReader(is, opts.charset().get()));
		return new CsvRecordSet(key, reader, opts);
	}
	
	static CsvRecordSet from(String key, BufferedReader reader, CsvParameters opts)
		throws IOException {
		Utilities.checkNotNullArgument(reader, "reader is null");
		Utilities.checkNotNullArgument(opts, "CsvOptions is null");
		
		return new CsvRecordSet(key, reader, opts);
	}
	
	static CsvRecordSet from(File file, CsvParameters opts) throws IOException {
		Utilities.checkNotNullArgument(file, "file is null");
		Utilities.checkNotNullArgument(opts, "CsvOptions is null");
		
		Reader reader = new InputStreamReader(new FileInputStream(file), opts.charset().get());
		return new CsvRecordSet(file.getAbsolutePath(), new BufferedReader(reader), opts);
	}
	
	private CsvRecordSet(String key, BufferedReader reader, CsvParameters opts) throws IOException {
		m_key = key;
		m_options = opts;
		setLogger(s_logger);
		
		m_nullValue = opts.nullValue().getOrNull();
		
		CSVFormat format = CSVFormat.DEFAULT.withDelimiter(opts.delimiter())
									.withQuote(null);
		format = opts.quote().transform(format, (f,q) -> f.withQuote(q));
		format = opts.escape().transform(format, (f,esc) -> f.withEscape(esc));
		if ( opts.trimColumn().getOrElse(false) ) {
			format = format.withTrim(true).withIgnoreSurroundingSpaces(true);
		}
		else {
			format = format.withTrim(false).withIgnoreSurroundingSpaces(false);
		}
		m_parser = format.parse(reader);
		
		m_iter = m_parser.iterator();
		if ( !m_iter.hasNext() ) {
			throw new IllegalArgumentException("input CSV file is empty: key=" + m_key);
		}
		m_first = Lists.newArrayList(m_iter.next().iterator());
		
		if ( opts.headerFirst().getOrElse(false) ) {
			m_schema = CsvUtils.buildRecordSchema(m_first);
			m_first = null;
		}
		else if ( opts.header().isPresent() ) {
			try ( Reader hdrReader = new StringReader(opts.header().getUnchecked());
					CSVParser hdrParser = format.parse(hdrReader); ) {
				CSVRecord header = hdrParser.getRecords().get(0);
				m_schema = CsvUtils.buildRecordSchema(Lists.newArrayList(header.iterator()));
			}
		}
		else {
			List<String> header = FStream.range(0, m_first.size())
									.map(idx -> String.format("field_%02d", idx))
									.toList();
			m_schema = CsvUtils.buildRecordSchema(header);
		}
		m_columns = m_schema.getColumns().toArray(new Column[0]);
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
			++m_lineNo;
			set(output, m_first);
			m_first = null;
			
			return true;
		}
		
		try {
			if ( !m_iter.hasNext() ) {
				return false;
			}
			
			++m_lineNo;
			List<String> values = Lists.newArrayList(m_iter.next().iterator());
			if ( values.size() != m_columns.length ) {
				String msg = String.format("invalid CSV line: %s:%d, expected=%d, csv=%s",
											m_key, m_lineNo, values.size(), m_columns.length, values);
				throw new IOException(msg);
			}
			set(output, values);
			
			return true;
		}
		catch ( Exception e ) {
			throw new RecordSetException("" + e);
		}
	}
	
	@Override
	public String toString() {
		return String.format("%s[%s]", getClass().getSimpleName(), m_options);
	}
	
	private void set(Record output, List<String> values) {
		for ( int i =0; i < values.size(); ++i ) {
			String value = values.get(i);

			// 길이 0 문자열을 null로 간주한다.
			// 이 경우 'null_value' 옵션이 설정된 경우 해당 값으로 치환시킨다.
			if ( value.length() == 0 ) {
				value = m_nullValue;
			}
			if ( m_columns[i].type() != DataType.STRING ) {
				output.set(i, DataUtils.cast(value, m_columns[i].type()));
			}
			else {
				output.set(i, value);
			}
		}
	}
}