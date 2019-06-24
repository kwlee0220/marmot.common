package marmot.geo.geotools;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.Map;

import org.geotools.data.FileDataStoreFinder;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
import org.geotools.data.shapefile.dbf.DbaseFileHeader;
import org.geotools.data.shapefile.dbf.DbaseFileReader;
import org.geotools.data.shapefile.files.ShpFiles;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.opengis.feature.simple.SimpleFeatureType;

import com.google.common.collect.Maps;

import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.RecordSetException;
import marmot.rset.AbstractRecordSet;
import utils.func.Try;
import utils.func.Unchecked;
import utils.io.FileUtils;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Shapefile {
	private final File m_file;
	private final Charset m_charset;
	
	public static Shapefile of(File file, Charset charset) {
		return new Shapefile(file, charset);
	}
	
	private Shapefile(File file, Charset charset) {
		m_file = file;
		m_charset = charset;
	}
	
	public File getFile() {
		return m_file;
	}
	
	public Charset getCharset() {
		return m_charset;
	}
	
	public RecordSchema getRecordSchema() throws IOException {
		ShapefileDataStore store = loadDataStore(m_file, m_charset);
		try {
			return SimpleFeatures.toRecordSchema(store.getSchema());
		}
		finally {
			store.dispose();
		}
	}
	
	public RecordSet read() throws IOException {
		return new RecordSetImpl(loadDataStore(m_file, m_charset));
	}
	
	public static Shapefile create(File shpFile, SimpleFeatureType type,
											Charset charset, boolean createIndex)
		throws IOException {
		Map<String,Serializable> params = Maps.newHashMap();
		params.put(ShapefileDataStoreFactory.URLP.key, shpFile.toURI().toURL());
		params.put(ShapefileDataStoreFactory.DBFCHARSET.key, charset.name());
		params.put(ShapefileDataStoreFactory.CREATE_SPATIAL_INDEX.key, createIndex);

		ShapefileDataStoreFactory fact = new ShapefileDataStoreFactory();
		ShapefileDataStore store = (ShapefileDataStore)fact.createNewDataStore(params);
		store.createSchema(type);

		return new Shapefile(shpFile, charset);
	}
	
	public int getRecordCount() throws IOException {
		return readDbfHeader(m_file, m_charset).getNumRecords();
	}
	
	@Override
	public String toString() {
		return m_file.toString();
	}
	
	private static ShapefileDataStore loadDataStore(File file, Charset charset)
		throws IOException {
		ShapefileDataStore store = (ShapefileDataStore)FileDataStoreFinder.getDataStore(file);
		store.setCharset(charset);
		
		return store;
	}
	
	private static DbaseFileHeader readDbfHeader(File file, Charset charset) throws IOException {
		DbaseFileReader reader = null;
		try {
			ShpFiles shpFile = new ShpFiles(file);
			reader = new DbaseFileReader(shpFile, false, charset);
			return reader.getHeader();
		}
		finally {
			if ( reader != null ) {
				Unchecked.runSneakily(reader::close);
			}
		}
	}
	
	public static FStream<File> traverseFiles(File start, Charset charset) throws IOException {
		return FileUtils.walk(start, "**/*.shp");
	}
	
	public static FStream<Shapefile> traverse(File start, Charset charset) throws IOException {
		return traverseFiles(start, charset)
						.map(file -> of(file, charset));
//						.flatMapTry(file -> Try.supply(() -> of(file, charset)));
	}
	
	private static class RecordSetImpl extends AbstractRecordSet {
		private final ShapefileDataStore m_store;
		private final SimpleFeatureRecordSet m_sfRSet;
		
		private RecordSetImpl(ShapefileDataStore store) {
			m_store = store;
			try {
				SimpleFeatureCollection sfColl = m_store.getFeatureSource().getFeatures();
				m_sfRSet = new SimpleFeatureRecordSet(sfColl);
			}
			catch ( Exception e ) {
				throw new RecordSetException("fails to read shapefile, cause=" + e);
			}
		}

		@Override
		protected void closeInGuard() {
			m_sfRSet.closeQuietly();
			Try.run(m_store::dispose);
			
			super.close();
		}

		@Override
		public RecordSchema getRecordSchema() {
			return m_sfRSet.getRecordSchema();
		}

		@Override
		public boolean next(Record record) {
			return m_sfRSet.next(record);
		}
	}
	
//	ShpFiles shapeFiles = new ShpFiles(shpFile);
//	System.out.println(shapeFiles.getFileNames());
//	System.out.println(shapeFiles.exists(ShpFileType.PRJ));
//	
//	File prjFile = new File(shapeFiles.get(ShpFileType.PRJ).substring(5));
//	System.out.println(prjFile.exists());
//	
//	try ( FileInputStream is = new FileInputStream(prjFile) ) {
//		PrjFileReader reader = new PrjFileReader(is.getChannel());
//		CoordinateReferenceSystem crs = reader.getCoordinateReferenceSystem();
//		System.out.println(crs);
//	}
}
