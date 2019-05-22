package marmot.externio.csv;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.RecordSchema;
import marmot.RecordSetException;
import marmot.rset.ConcatedRecordSet;
import utils.Utilities;
import utils.io.FileUtils;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class MultiFileCsvRecordSet extends ConcatedRecordSet {
	private static final Logger s_logger = LoggerFactory.getLogger(MultiFileCsvRecordSet.class);
	
	private final File m_start;
	private final FStream<File> m_files;
	private final CsvParameters m_options;
	private CsvRecordSet m_first;
	private final RecordSchema m_schema;
	
	MultiFileCsvRecordSet(File start, CsvParameters params) {
		Utilities.checkNotNullArgument(start, "start is null");
		Utilities.checkNotNullArgument(params, "params is null");
		
		m_start = start;
		setLogger(s_logger);
		
		try {
			List<File> files = FileUtils.walk(start, "**/*.csv").toList();
			if ( files.isEmpty() ) {
				throw new IllegalArgumentException("no CSV files to read: path=" + start);
			}
			
			getLogger().info("loading CSVFile: from={}, nfiles={}", start, files.size());

			m_files = FStream.from(files);
			m_options = params;
			
			m_first = loadNext();
			m_schema = m_first.getRecordSchema();
		}
		catch ( IOException e ) {
			throw new RecordSetException("fails to parse CSV, cause=" + e);
		}
	}
	
	@Override
	protected void closeInGuard() {
		if ( m_first != null ) {
			m_first.closeQuietly();
			m_first = null;
		}
		m_files.closeQuietly();
		
		super.closeInGuard();
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_schema;
	}
	
	@Override
	public String toString() {
		return String.format("%s[start=%s]params[%s]", getClass().getSimpleName(), m_start, m_options);
	}

	@Override
	protected CsvRecordSet loadNext() {
		if ( m_first != null ) {
			CsvRecordSet rset = m_first;
			m_first = null;
			
			return rset;
		}
		else {
			return m_files.next().map(this::loadFile).getOrNull();
		}
	}
	
	private CsvRecordSet loadFile(File file) {
		try {
			CsvRecordSet rset = CsvRecordSet.from(file, m_options);
			getLogger().info("loading: CSV[{}], from={}", m_options, file);
			
			return rset;
		}
		catch ( IOException e ) {
			getLogger().warn("fails to load CsvRecordSet: from=" + file
							+ ", cause=" + e);
			throw new RecordSetException(e);
		}
	}
}