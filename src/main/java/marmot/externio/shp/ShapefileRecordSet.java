package marmot.externio.shp;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import org.geotools.data.FileDataStoreFinder;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vavr.control.Try;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSetException;
import marmot.geo.geotools.GeoToolsUtils;
import marmot.geo.geotools.SimpleFeatureRecordSet;
import marmot.rset.AbstractRecordSet;
import marmot.rset.ConcatedRecordSet;
import utils.io.FileUtils;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ShapefileRecordSet extends ConcatedRecordSet {
	private static final Logger s_logger = LoggerFactory.getLogger(ShapefileRecordSet.class);
	
	private final File m_start;
	private final Charset m_charset;
	
	private final FStream<File> m_files;
	private final RecordSchema m_schema;
	
	public ShapefileRecordSet(File start, Charset charset) {
		m_start = start;
		m_charset = charset;
		setLogger(s_logger);
		
		try {
			List<File> files = FileUtils.walk(start, "**/*.shp").toList();
			if ( files.isEmpty() ) {
				throw new IllegalArgumentException("no Shapefiles to read: path=" + start);
			}
			m_files = FStream.from(files);
			
			ShapefileDataStore store = loadDataStore(files.get(0), m_charset);
			try {
				SimpleFeatureType sfType = store.getSchema();
				m_schema = GeoToolsUtils.toRecordSchema(sfType);
			}
			finally {
				store.dispose();
			}
			
			getLogger().info("loading {}: nfiles={}", this, files.size());
		}
		catch ( Exception e ) {
			throw new RecordSetException("fails to parse Shapefile", e);
		}
		
	}

	@Override
	protected void closeInGuard() {
		m_files.closeQuietly();
		
		super.close();
	}
	
	@Override
	public RecordSchema getRecordSchema() {
		return m_schema;
	}

	public static FStream<File> collectShapefile(File start) throws IOException {
		if ( start.isDirectory() ) {
			return FileUtils.walk(start, "**/*.shp");
		}
		else {
			return FStream.of(start);
		}
	}
	
	@Override
	public String toString() {
		return String.format("Shapefiles[path=%s]", m_start);
	}

	@Override
	protected InnerRecordSet loadNext() {
		return m_files.next()
						.map(this::loadRecordSet)
						.getOrNull();
	}
	private InnerRecordSet loadRecordSet(File file) {
		getLogger().info("loading path={}", file);
		return new InnerRecordSet(file);
	}
	
	public static RecordSchema loadRecordSchema(File start, Charset charset) throws IOException {
		ShapefileDataStore store = FileUtils.walk(start, "**/*.shp")
											.map(file -> Try.of(() -> loadDataStore(file, charset)))
											.filter(Try::isSuccess)
											.map(Try::get)
											.next()
											.getOrNull();
		if ( store == null ) {
			throw new IllegalArgumentException("no valid Shapefile: path=" + start);
		}
		try {
			store.setCharset(charset);
			return GeoToolsUtils.toRecordSchema(store.getSchema());
		}
		finally {
			store.dispose();
		}
	}
	
	public static ShapefileDataStore loadDataStore(File shpFile, Charset charset) throws IOException {
		ShapefileDataStore store = (ShapefileDataStore)FileDataStoreFinder.getDataStore(shpFile);
		store.setCharset(charset);
		
		return store;
	}
	
	class InnerRecordSet extends AbstractRecordSet {
		private final File m_shpFile;
		private ShapefileDataStore m_store = null;
		private final SimpleFeatureRecordSet m_sfRSet;
		
		private InnerRecordSet(File shpFile) {
			m_shpFile = shpFile;
			
			try {
				m_store = loadDataStore(m_shpFile, m_charset);
				
				SimpleFeatureCollection sfColl = m_store.getFeatureSource().getFeatures();
				m_sfRSet = new SimpleFeatureRecordSet(sfColl);
			}
			catch ( Exception e ) {
				if ( m_store != null ) {
					m_store.dispose();
				}
				
				throw new RecordSetException("fails to read shapefile, cause=" + e);
			}
		}

		@Override
		protected void closeInGuard() {
			Try.run(m_sfRSet::close);
			Try.run(m_store::dispose);
			
			super.close();
		}

		@Override
		public RecordSchema getRecordSchema() {
			return m_schema;
		}

		@Override
		public boolean next(Record record) {
			return m_sfRSet.next(record);
		}
	}
}
