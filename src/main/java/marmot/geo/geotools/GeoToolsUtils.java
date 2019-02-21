package marmot.geo.geotools;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.geotools.data.FeatureSource;
import org.geotools.data.FileDataStoreFinder;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
import org.geotools.data.shapefile.dbf.DbaseFileHeader;
import org.geotools.data.shapefile.dbf.DbaseFileReader;
import org.geotools.data.shapefile.files.ShpFiles;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geometry.jts.Geometries;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.referencing.FactoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.vividsolutions.jts.geom.Geometry;

import io.vavr.control.Option;
import marmot.DataSet;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.RecordSetException;
import marmot.geo.GeoClientUtils;
import marmot.type.DataType;
import marmot.type.DataTypes;
import utils.Unchecked;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GeoToolsUtils {
	static final Logger s_logger = LoggerFactory.getLogger(GeoToolsUtils.class);
	
	private GeoToolsUtils() {
		throw new AssertionError("Should not be called: " + getClass().getName());
	}
	
	public static SimpleFeatureRecordSet toRecordSet(FeatureIterator<SimpleFeature> iter) {
		try {
			return new SimpleFeatureRecordSet(iter);
		}
		catch ( FactoryException e ) {
			throw new RecordSetException("cause=" + e);
		}
	}
	
	public static SimpleFeatureRecordSet toRecordSet(
									FeatureCollection<SimpleFeatureType,SimpleFeature> sfColl) {
		try {
			return new SimpleFeatureRecordSet(sfColl);
		}
		catch ( FactoryException e ) {
			throw new RecordSetException("cause=" + e);
		}
	}
	
	public static SimpleFeatureRecordSet toRecordSet(
											FeatureSource<SimpleFeatureType,SimpleFeature> sfSrc) {
		try {
			return toRecordSet(sfSrc.getFeatures());
		}
		catch ( IOException e ) {
			throw new RecordSetException("cause=" + e);
		}
	}
	
	public static SimpleFeatureType toSimpleFeatureType(String sfTypeName, String srid,
														RecordSchema schema) {
		SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
		builder.setName(sfTypeName);
		builder.setSRS(srid);
		
		Map<String,Integer> abbrs = Maps.newHashMap();
		schema.forEach(col -> {
			String colName = col.name();
			if ( colName.length() > 10 ) {
				colName = colName.substring(0, 9);
				int seqno = abbrs.getOrDefault(colName, 0);
				abbrs.put(colName, (seqno+1));
				colName += (""+seqno);
				
				s_logger.warn(String.format("truncate too long field name: %s->%s", col.name(), colName));
			}
			
			switch ( col.type().getTypeCode() ) {
				case DATE:
				case DATETIME:
					builder.add(colName, Date.class);
					break;
				case INTERVAL:
					builder.add(colName, String.class);
					break;
				case GRID_CELL:
					builder.add(colName, String.class);
					break;
				default:
					builder.add(colName, col.type().getInstanceClass());
					break;
			}
		});
		return builder.buildFeatureType();
	}
	
	public static SimpleFeatureType toSimpleFeatureType(String sfTypeName, DataSet ds) {
		return toSimpleFeatureType(sfTypeName, ds.getGeometryColumnInfo().srid(),
									ds.getRecordSchema());
	}
	
	/**
	 * SimpleFeatureType로부터 RecordSchema 객체를 생성한다.
	 * 
	 * @param sfType	SimpleFeatureType 타입
	 * @return	RecordSchema
	 */
	public static RecordSchema toRecordSchema(SimpleFeatureType sfType) {
		RecordSchema.Builder builder = RecordSchema.builder();
		for ( AttributeDescriptor desc: sfType.getAttributeDescriptors() ) {
			Class<?> instCls = desc.getType().getBinding();
			DataType attrType = DataTypes.fromInstanceClass(instCls);
			builder.addColumn(desc.getLocalName(), attrType);
		}
		
		return builder.build();
	}
	
	public static Function<Record,SimpleFeature> createToSimpleFeature(String typeName,
													String srs, RecordSchema schema) {
		return new ToSimpleFeture(typeName, srs, schema);
	}
	
	public static ShapefileDataStore createShapefileDataStore(File shpFile,
													SimpleFeatureType type,
													Charset charset, boolean createIndex)
		throws IOException {
		Map<String,Serializable> params = Maps.newHashMap();
		params.put(ShapefileDataStoreFactory.URLP.key, shpFile.toURI().toURL());
		params.put(ShapefileDataStoreFactory.DBFCHARSET.key, charset.name());
		params.put(ShapefileDataStoreFactory.CREATE_SPATIAL_INDEX.key, createIndex);

		ShapefileDataStoreFactory fact = new ShapefileDataStoreFactory();
		ShapefileDataStore store = (ShapefileDataStore)fact.createNewDataStore(params);
		store.createSchema(type);

		return store;
	}
	
	public static SimpleFeatureCollection toFeatureCollection(String sfTypeName, DataSet ds) {
		SimpleFeatureType sfType = toSimpleFeatureType(sfTypeName, ds);
		return new MarmotFeatureCollection(sfType, () -> ds.read());
	}
	
	public static SimpleFeatureCollection toFeatureCollection(String sfTypeName,
															MarmotRuntime marmot, Plan plan, String srid) {
		RecordSchema schema = marmot.getOutputRecordSchema(plan);
		SimpleFeatureType sfType = toSimpleFeatureType(sfTypeName, srid, schema);
		return new MarmotFeatureCollection(sfType, () -> marmot.executeToRecordSet(plan));
	}
	
	public static SimpleFeatureCollection toFeatureCollection(String sfTypeName,
													RecordSchema schema, String srid,
													Iterable<Record> records) {
		SimpleFeatureType sfType = toSimpleFeatureType(sfTypeName, srid, schema);
		return new MarmotFeatureCollection(sfType, () -> RecordSet.from(records));
	}
	
	public static ShapefileDataStore openShapefileDataStore(File shpFile) throws IOException {
		return (ShapefileDataStore)FileDataStoreFinder.getDataStore(shpFile);
	}
	
	public static RecordSchema getRecordSchema(File shpFile) throws IOException {
		ShapefileDataStore store = openShapefileDataStore(shpFile);
		store.setCharset(Charset.forName("utf-8"));
		SimpleFeatureType type = store.getSchema();
		return toRecordSchema(type);
	}
	
	public static Stream<File> streamShapeFiles(File file) throws IOException {
		return Files.walk(file.toPath())
					.filter(p -> p.toString().endsWith(".shp"))
					.map(Path::toFile);
	}
	
	/**
	 * 주어진 파일 또는 디렉토리인 경우는 모든 하위 파일들 중에서 확장자가 '.shp'인 파일을
	 * 모든 검색한다.
	 * 
	 *  @param	file	검색 대상 파일 (또는 디렉토리)
	 *  @return	검색된 파일 리스트.
	 *  @throws	IOException	검색 과정 중 IO 오류가 발생된 경우.
	 */
	public static List<File> collectShapeFiles(File file) throws IOException {
		return streamShapeFiles(file).collect(Collectors.toList());
	}
	
	public static Option<Geometry> validateGeometry(Geometry geom) {
		if ( !geom.isValid() ) {
			Geometries type = Geometries.get(geom);
			
			geom = GeoClientUtils.makeValid(geom);
			if ( !geom.isEmpty() && geom.isValid() ) {
				geom = GeoClientUtils.cast(geom, type);
			}
			else {
				return Option.none();
			}
		}
		
		return Option.some(geom);
	}
	
	public static int countShpRecords(File file, Charset cs) throws IOException {
		return streamShapeFiles(file)
				.map(shpFile -> readDbfHeader(shpFile, cs))
				.mapToInt(DbaseFileHeader::getNumRecords)
				.sum();
	}
	
	private static DbaseFileHeader readDbfHeader(File file, Charset cs) {
		DbaseFileReader reader = null;
		try {
			ShpFiles shpFile = new ShpFiles(file);
			reader = new DbaseFileReader(shpFile, false, cs);
			return reader.getHeader();
		}
		catch ( IOException e ) {
			throw new RecordSetException(e);
		}
		finally {
			if ( reader != null ) {
				Unchecked.runRTE(reader::close);
			}
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
