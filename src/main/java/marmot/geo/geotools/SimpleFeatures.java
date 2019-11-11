package marmot.geo.geotools;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import org.geotools.data.FeatureSource;
import org.geotools.data.collection.ListFeatureCollection;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.feature.collection.BaseSimpleFeatureCollection;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.referencing.FactoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import marmot.DataSet;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.RecordSetException;
import marmot.support.DefaultRecord;
import marmot.type.DataType;
import marmot.type.DataTypes;
import utils.Utilities;
import utils.io.FileUtils;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SimpleFeatures {
	static final Logger s_logger = LoggerFactory.getLogger(SimpleFeatures.class);
	
	private SimpleFeatures() {
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
		schema.streamColumns()
				.forEach(col -> {
					String colName = col.name();
					if ( colName.length() > 10 ) {
						colName = colName.substring(0, 9);
						int seqno = abbrs.getOrDefault(colName, 0);
						abbrs.put(colName, (seqno+1));
						colName += (""+seqno);
						
						s_logger.warn(String.format("truncate too long field name: %s->%s",
													col.name(), colName));
					}
					
					switch ( col.type().getTypeCode() ) {
						case STRING:
							builder.nillable(true).add(colName, String.class);
							break;
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
		Utilities.checkNotNullArgument(sfType, "feature type is null");
		
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
	
	public static SimpleFeatureCollection toFeatureCollection(DataSet ds) {
		final SimpleFeatureType sfType = SimpleFeatures.toSimpleFeatureType(ds.getId(), ds);
		return new BaseSimpleFeatureCollection(sfType) {
			@Override
			public SimpleFeatureIterator features() {
				return new MarmotFeatureIterator(getSchema(), ds.read());
			}
		};
	}
	
	public static SimpleFeatureCollection toFeatureCollection(String sfTypeName, String srid,
																RecordSet rset) {
		final SimpleFeatureType sfType = toSimpleFeatureType("main", srid, rset.getRecordSchema());
		return new ListFeatureCollection(sfType, toFeatureList("main", sfType, rset));
	}
	
	public static SimpleFeatureCollection toFeatureCollection(SimpleFeatureType sfType,
																Iterable<Record> records) {
		return new ListFeatureCollection(sfType, toFeatureList("main", sfType, records));
	}
	
	public static SimpleFeatureCollection toFeatureCollection(String sfTypeName,
													MarmotRuntime marmot, Plan plan, String srid) {
		RecordSchema schema = marmot.getOutputRecordSchema(plan);
		return toFeatureCollection(sfTypeName, srid, schema, () -> marmot.executeToRecordSet(plan));
	}
	
	public static SimpleFeatureCollection toFeatureCollection(String sfTypeName, String srid,
												RecordSchema schema, Supplier<RecordSet> supplier) {
		final SimpleFeatureType sfType = SimpleFeatures.toSimpleFeatureType(sfTypeName, srid, schema);
		return new BaseSimpleFeatureCollection(sfType) {
			@Override
			public SimpleFeatureIterator features() {
				return new MarmotFeatureIterator(getSchema(), supplier.get());
			}
		};
	}
	
	public static List<SimpleFeature> toFeatureList(String sfTypeName, SimpleFeatureType sfType,
													RecordSet rset) {
		SimpleFeatureBuilder builder = new SimpleFeatureBuilder(sfType);
		
		List<SimpleFeature> features = Lists.newArrayList();
		Record record = DefaultRecord.of(rset.getRecordSchema());
		while ( rset.next(record) ) {
			features.add(builder.buildFeature(null, record.getAll()));
		}
		
		return features;
	}
	
	public static List<SimpleFeature> toFeatureList(String sfTypeName, SimpleFeatureType sfType,
													Iterable<Record> records) {
		SimpleFeatureBuilder builder = new SimpleFeatureBuilder(sfType);
		return FStream.from(records).map(r -> builder.buildFeature(null, r.getAll())).toList();
	}
	
	public static FStream<File> streamShapeFiles(File start) throws IOException {
		return FileUtils.walk(start, "**/*.shp");
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
