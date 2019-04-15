package marmot.externio.csv;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.util.Objects;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import io.reactivex.Observable;
import io.reactivex.subjects.BehaviorSubject;
import marmot.Column;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.externio.RecordSetWriter;
import marmot.support.DefaultRecord;
import utils.UnitUtils;
import utils.async.ProgressReporter;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CsvRecordSetWriter implements RecordSetWriter, ProgressReporter<Long> {
	private static final int DEFAULT_BUFFER_SIZE = (int)UnitUtils.parseByteSize("64kb");
	
	private final BufferedWriter m_writer;
	private final CsvParameters m_params;
	private long m_interval = -1L;
	private final BehaviorSubject<Long> m_subject = BehaviorSubject.create();
	
	public static CsvRecordSetWriter get(File file, CsvParameters params) throws IOException {
		return new CsvRecordSetWriter(Files.newBufferedWriter(file.toPath(), params.charset()),
										params);
	}
	
	public static CsvRecordSetWriter get(Writer writer, CsvParameters params)
		throws IOException {
		BufferedWriter bwriter = (writer instanceof BufferedWriter)
								? (BufferedWriter)writer
								: new BufferedWriter(writer, DEFAULT_BUFFER_SIZE);
		return new CsvRecordSetWriter(bwriter, params);
	}
	
	public static CsvRecordSetWriter get(OutputStream os, CsvParameters params) throws IOException {
		Writer writer = new OutputStreamWriter(os, params.charset());
		return new CsvRecordSetWriter(new BufferedWriter(writer, DEFAULT_BUFFER_SIZE), params);
	}
	
	public CsvRecordSetWriter(BufferedWriter writer, CsvParameters params) {
		Objects.requireNonNull(writer, "writer is null");
		Objects.requireNonNull(params, "CsvParameters is null");
		
		m_writer = writer;
		m_params = params;
	}

	@Override
	public void close() throws IOException {
		m_subject.onComplete();
		m_writer.close();
	}
	
	public CsvRecordSetWriter progressInterval(long interval) {
		m_interval = interval;
		return this;
	}

	@Override
	public long write(RecordSet rset) throws IOException {
		Objects.requireNonNull(rset, "RecordSet is null");
		
		CSVFormat format = CSVFormat.DEFAULT.withQuote(null).withIgnoreSurroundingSpaces();
		format.withDelimiter(m_params.delimiter());
		m_params.quote().ifPresent(format::withQuote);
		m_params.escape().ifPresent(format::withEscape);
		format.withSkipHeaderRecord(!m_params.headerFirst());

		RecordSchema schema = rset.getRecordSchema();
		String[] header = schema.streamColumns()
								.map(Column::name)
								.toArray(String.class);
		format = format.withHeader(header);
		
		Column[] cols = schema.streamColumns().toArray(Column.class);
		CSVPrinter printer = format.print(m_writer);
		
		long nrecs = 0;
		Record record = DefaultRecord.of(schema);
		while ( rset.next(record) ) {
			printer.printRecord(toCsvPartString(cols, record));
			++nrecs;
			if ( m_interval > 0 &&  (nrecs % m_interval) == 0 ) {
				m_subject.onNext(nrecs);
			}
		}
		
		return nrecs;
	}

	@Override
	public Observable<Long> getProgressObservable() {
		return m_subject;
	}
	
	private static Object[] toCsvPartString(Column[] cols, Record record) {
		Object[] parts = new Object[cols.length];
		for ( int i = 0; i < parts.length; ++i ) {
			Object value = record.get(i);
			parts[i] = (value != null) ? cols[i].type().toInstanceString(value) : "";
		}
		
		return parts;
	}
}
