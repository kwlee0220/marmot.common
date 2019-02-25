package marmot.geo.geotools;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

import org.geotools.data.FileDataStoreFinder;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.shapefile.dbf.DbaseFileHeader;
import org.geotools.data.shapefile.dbf.DbaseFileReader;
import org.geotools.data.shapefile.files.ShpFiles;
import org.geotools.data.simple.SimpleFeatureCollection;

import io.vavr.control.Try;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.RecordSetException;
import marmot.rset.AbstractRecordSet;
import utils.Unchecked;
import utils.io.FileUtils;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Shapefile {
	private final File m_file;
	private final Charset m_charset;
	
	private Shapefile(File file, Charset charset) throws IOException {
		m_file = file;
		m_charset = charset;
	}
	
	public RecordSchema getRecordSchema() throws IOException {
		return toRecordSchema(loadDataStore());
	}
	
	public RecordSet loadRecordSet() {
		return new RecordSetImpl();
	}
	
	public int getRecordCount() throws IOException {
		return readDbfHeader().getNumRecords();
	}
	
	public ShapefileDataStore loadDataStore() throws IOException {
		ShapefileDataStore store = (ShapefileDataStore)FileDataStoreFinder.getDataStore(m_file);
		store.setCharset(m_charset);
		
		return store;
	}
	
	private static RecordSchema toRecordSchema(ShapefileDataStore store) throws IOException {
		return GeoToolsUtils.toRecordSchema(store.getSchema());
	}
	
	private DbaseFileHeader readDbfHeader() throws IOException {
		DbaseFileReader reader = null;
		try {
			ShpFiles shpFile = new ShpFiles(m_file);
			reader = new DbaseFileReader(shpFile, false, m_charset);
			return reader.getHeader();
		}
		finally {
			if ( reader != null ) {
				Unchecked.runRTE(reader::close);
			}
		}
	}
	
	public static FStream<File> streamShapeFiles(File start) throws IOException {
		return FileUtils.walk(start, "**/*.shp");
	}
	
	class RecordSetImpl extends AbstractRecordSet {
		private final ShapefileDataStore m_store;
		private final SimpleFeatureRecordSet m_sfRSet;
		
		private RecordSetImpl() {
			ShapefileDataStore store = null;
			try {
				store = loadDataStore();
				
				SimpleFeatureCollection sfColl = store.getFeatureSource().getFeatures();
				m_store = store;
				m_sfRSet = new SimpleFeatureRecordSet(sfColl);
			}
			catch ( Exception e ) {
				if ( store != null ) {
					store.dispose();
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
