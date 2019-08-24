package marmot.externio.csv;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import io.reactivex.Observable;
import io.reactivex.subjects.BehaviorSubject;
import marmot.Column;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.externio.RecordWriter;
import marmot.optor.StoreAsCsvOptions;
import marmot.type.DataType;
import utils.UnitUtils;
import utils.Utilities;
import utils.async.ProgressReporter;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CsvRecordWriter implements RecordWriter, ProgressReporter<Long> {
	private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
	private static final int DEFAULT_BUFFER_SIZE = (int)UnitUtils.parseByteSize("64kb");

	private final CSVPrinter m_printer;
	private final RecordSchema m_schema;
	private final DataType[] m_colTypes;
	private long m_interval = -1L;
	private final BehaviorSubject<Long> m_subject = BehaviorSubject.create();
	private long m_count = 0;
	
	public static CsvRecordWriter get(File file, RecordSchema schema, StoreAsCsvOptions opts)
		throws IOException {
		Charset cs = opts.charset().getOrElse(DEFAULT_CHARSET);
		return new CsvRecordWriter(Files.newBufferedWriter(file.toPath(), cs), schema, opts);
	}
	
	public static CsvRecordWriter get(Writer writer, RecordSchema schema, StoreAsCsvOptions opts)
		throws IOException {
		BufferedWriter bwriter = (writer instanceof BufferedWriter)
								? (BufferedWriter)writer
								: new BufferedWriter(writer, DEFAULT_BUFFER_SIZE);
		return new CsvRecordWriter(bwriter, schema, opts);
	}
	
	public static CsvRecordWriter get(OutputStream os, RecordSchema schema, StoreAsCsvOptions opts)
		throws IOException {
		Charset cs = opts.charset().getOrElse(DEFAULT_CHARSET);
		
		Writer writer = new OutputStreamWriter(os, cs);
		return new CsvRecordWriter(new BufferedWriter(writer, DEFAULT_BUFFER_SIZE), schema, opts);
	}
	
	public static long write(Writer writer, RecordSet rset, StoreAsCsvOptions opts) throws IOException {
		try ( CsvRecordWriter csvWriter = get(writer, rset.getRecordSchema(), opts) ) {
			return csvWriter.write(rset);
		}
	}
	
	public static long write(OutputStream os, RecordSet rset, StoreAsCsvOptions opts) throws IOException {
		try ( CsvRecordWriter writer = get(os, rset.getRecordSchema(), opts) ) {
			return writer.write(rset);
		}
	}
	
	private CsvRecordWriter(BufferedWriter writer, RecordSchema schema, StoreAsCsvOptions opts)
		throws IOException {
		Utilities.checkNotNullArgument(writer, "writer is null");
		Utilities.checkNotNullArgument(opts, "StoreAsCsvOptions is null");
		Utilities.checkNotNullArgument(schema, "RecordSchema is null");
		
		m_schema = schema;
		
		CSVFormat format = CSVFormat.DEFAULT.withQuote(null)
											.withIgnoreSurroundingSpaces()
											.withDelimiter(opts.delimiter());
		format = opts.quote().transform(format, (f,q) -> f.withQuote(q));
		format = opts.escape().transform(format, (f,esc) -> f.withEscape(esc));

		String[] header = schema.streamColumns()
								.map(Column::name)
								.toArray(String.class);
		format = format.withHeader(header);
		format = format.withSkipHeaderRecord(!opts.headerFirst().getOrElse(false));
		
		m_printer = format.print(writer);
		m_colTypes = schema.streamColumns()
							.map(Column::type)
							.toArray(DataType.class);
	}

	@Override
	public void close() throws IOException {
		m_subject.onComplete();
		m_printer.close();
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_schema;
	}

	public void flush() throws IOException {
		m_printer.flush();
	}
	
	public CsvRecordWriter progressInterval(long interval) {
		m_interval = interval;
		return this;
	}

	@Override
	public void write(Record record) throws IOException {
		Utilities.checkNotNullArgument(record, "Record is null");
		
		m_printer.printRecord(toCsvPartString(record));
		if ( m_interval > 0 &&  (++m_count % m_interval) == 0 ) {
			m_subject.onNext(m_count);
		}
	}

	@Override
	public Observable<Long> getProgressObservable() {
		return m_subject;
	}
	
	private Object[] toCsvPartString(Record record) {
		Object[] parts = new Object[m_colTypes.length];
		for ( int i = 0; i < parts.length; ++i ) {
			Object value = record.get(i);
			parts[i] = (value != null) ? m_colTypes[i].toInstanceString(value) : "";
		}
		
		return parts;
	}
}
