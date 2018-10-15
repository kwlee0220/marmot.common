package marmot.externio.text;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vavr.control.Option;
import io.vavr.control.Try;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.RecordSetException;
import marmot.externio.geojson.MultiFileGeoJsonRecordSet;
import marmot.rset.AbstractRecordSet;
import marmot.rset.ConcatedRecordSet;
import marmot.type.DataType;
import utils.io.FileUtils;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TextLineFileRecordSet extends ConcatedRecordSet {
	private static final Logger s_logger = LoggerFactory.getLogger(MultiFileGeoJsonRecordSet.class);
	public static RecordSchema SCHEMA = RecordSchema.builder()
													.addColumn("text", DataType.STRING)
													.build();

	private final File m_start;
	private final TextLineParameters m_params;
	private final FStream<File> m_files;
	
	public TextLineFileRecordSet(File start, TextLineParameters params) {
		m_start = start;
		m_params = params;
		setLogger(s_logger);
		
		try {
			List<File> files = FileUtils.walk(start, m_params.glob()).toList();
			if ( files.isEmpty() ) {
				throw new IllegalArgumentException("no TextFile to read: path=" + start);
			}
			
			getLogger().info("loading TextFile: from={}, nfiles={}", start, files.size());

			m_files = FStream.of(files);
		}
		catch ( IOException e ) {
			throw new RecordSetException("fails to parse TextFile", e);
		}
	}

	@Override
	public RecordSchema getRecordSchema() {
		return SCHEMA;
	}
	
	@Override
	public String toString() {
		return String.format("%s[start=%s, glob='%s']", getClass().getSimpleName(),
							m_start, m_params.glob());
	}

	@Override
	protected RecordSet loadNext() {
		Option<File> next;
		while ( (next = m_files.next()).isDefined() ) {
			try {
				getLogger().debug("loading TextFile: {}", next.get());
				
				BufferedReader reader = Files.newBufferedReader(next.get().toPath(),
																m_params.charset());
				return new InnerRecordSet(reader);
			}
			catch ( IOException ignored ) { }
		}
		
		return null;
	}
	
	class InnerRecordSet extends AbstractRecordSet {
		private final BufferedReader m_reader;
		
		private InnerRecordSet(BufferedReader reader) {
			m_reader = reader;
		}
		
		@Override
		protected void closeInGuard() {
			Try.run(m_reader::close);
		}

		@Override
		public RecordSchema getRecordSchema() {
			return SCHEMA;
		}
		
		@Override
		public boolean next(Record output) {
			try {
				String marker = m_params.commentMarker().getOrNull();
				while ( true ) {
					String line = m_reader.readLine();
					if ( line == null ) {
						return false;
					}
					
					if ( marker == null || !line.startsWith(marker) ) {
						output.set(0, line);
						
						return true;
					}
				}
			}
			catch ( IOException e ) {
				throw new RecordSetException("" + e);
			}
		}
		
		@Override
		public String toString() {
			return String.format("%s", getClass().getSimpleName());
		}
	}
}
