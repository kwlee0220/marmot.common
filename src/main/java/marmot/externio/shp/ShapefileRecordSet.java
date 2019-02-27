package marmot.externio.shp;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vavr.control.Try;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.RecordSetException;
import marmot.geo.geotools.Shapefile;
import marmot.rset.ConcatedRecordSet;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ShapefileRecordSet extends ConcatedRecordSet {
	private static final Logger s_logger = LoggerFactory.getLogger(ShapefileRecordSet.class);
	
	private final File m_start;
	private final Charset m_charset;
	
	private final FStream<RecordSet> m_rsets;
	private final RecordSchema m_schema;
	
	public ShapefileRecordSet(File start, Charset charset) {
		m_start = start;
		m_charset = charset;
		setLogger(s_logger);
		
		try {
			List<Shapefile> files = Shapefile.traverse(start, charset).toList();
			if ( files.isEmpty() ) {
				throw new IllegalArgumentException("no Shapefiles to read: path=" + start);
			}
			
			m_schema = loadRecordSchema(start, charset);
			m_rsets = Shapefile.traverse(start, charset)
								.flatMapTry(shp -> {
									getLogger().info("loading shapefile: " + shp);
									return Try.of(() -> shp.read());
								});
			
			getLogger().info("loading {}: nfiles={}", this, files.size());
		}
		catch ( Exception e ) {
			throw new RecordSetException("fails to parse Shapefile", e);
		}
	}

	@Override
	protected void closeInGuard() {
		m_rsets.closeQuietly();
		
		super.close();
	}
	
	@Override
	public RecordSchema getRecordSchema() {
		return m_schema;
	}
	
	public Charset getCharset() {
		return m_charset;
	}
	
	@Override
	public String toString() {
		return String.format("Shapefiles[path=%s]", m_start);
	}

	@Override
	protected RecordSet loadNext() {
		return m_rsets.next().getOrNull();
	}
	
	public static RecordSchema loadRecordSchema(File start, Charset charset) throws IOException {
		return Shapefile.traverse(start, charset)
						.flatMapTry(shp -> Try.of(() -> shp.getRecordSchema()))
						.next()
						.getOrElseThrow(() -> new IllegalArgumentException("no valid shapefile to read: path=" + start));
	}
	
//	class InnerRecordSet extends AbstractRecordSet {
//		private final File m_shpFile;
//		private ShapefileDataStore m_store = null;
//		private final SimpleFeatureRecordSet m_sfRSet;
//		
//		private InnerRecordSet(File shpFile) {
//			m_shpFile = shpFile;
//			
//			try {
//				m_store = loadDataStore(m_shpFile, m_charset);
//				
//				SimpleFeatureCollection sfColl = m_store.getFeatureSource().getFeatures();
//				m_sfRSet = new SimpleFeatureRecordSet(sfColl);
//			}
//			catch ( Exception e ) {
//				if ( m_store != null ) {
//					m_store.dispose();
//				}
//				
//				throw new RecordSetException("fails to read shapefile, cause=" + e);
//			}
//		}
//
//		@Override
//		protected void closeInGuard() {
//			Try.run(m_sfRSet::close);
//			Try.run(m_store::dispose);
//			
//			super.close();
//		}
//
//		@Override
//		public RecordSchema getRecordSchema() {
//			return m_schema;
//		}
//
//		@Override
//		public boolean next(Record record) {
//			return m_sfRSet.next(record);
//		}
//	}
}
