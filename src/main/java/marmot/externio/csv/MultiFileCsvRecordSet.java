package marmot.externio.csv;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipInputStream;

import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

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
	private final CsvParameters m_params;
	private final FStream<File> m_files;
	private CsvRecordSet m_first;
	private final RecordSchema m_schema;
	
	MultiFileCsvRecordSet(File start, CsvParameters params) {
		this(start, params, "**/*.csv");
	}
	
	MultiFileCsvRecordSet(File start, CsvParameters params, String glob) {
		Utilities.checkNotNullArgument(start, "start is null");
		Utilities.checkNotNullArgument(params, "params is null");
		Utilities.checkNotNullArgument(glob, "glob is null");
		
		m_start = start;
		setLogger(s_logger);
		
		try {
			List<File> files;
			if ( start.isDirectory() ) {
//				String glob = "**/*.{csv,gz,gzip,zip}";
				files = FileUtils.walk(start, glob)
								.sort()
								.toList();
				if ( files.isEmpty() ) {
					throw new IllegalArgumentException("no CSV files to read: path=" + start);
				}
			}
			else {
				files = Lists.newArrayList(start);
			}
			if ( files.isEmpty() ) {
				throw new IllegalArgumentException("no CSV files to read: path=" + start);
			}
			getLogger().info("loading CSVFile: from={}, nfiles={}", start, files.size());

			m_files = FStream.from(files);
			m_params = params;
			
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
		return String.format("%s[start=%s]params[%s]", getClass().getSimpleName(), m_start, m_params);
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
	
	@SuppressWarnings("resource")
	private CsvRecordSet loadFile(File file) {
		try {
			InputStream src = new FileInputStream(file);
			String ext = FilenameUtils.getExtension(file.getAbsolutePath());
			switch ( ext ) {
				case "csv":
					break;
				case "gz":
				case "gzip":
					src = new GZIPInputStream(src);
					break;
				case "zip":
					src = new ZipInputStream(src);
					break;
				default:
					String msg = String.format("fails to load CsvRecordSet: unknown extenstion=%s", ext);
					throw new RecordSetException(msg);
			}
			
			CsvRecordSet rset = CsvRecordSet.from(file.getAbsolutePath(), src, m_params);
			getLogger().info("loading: CSV[{}], {}", m_params, file);
			
			return rset;
		}
		catch ( IOException e ) {
			String msg = String.format("fails to load CsvRecordSet: %s", file);
			throw new RecordSetException(msg, e);
		}
	}
}