package marmot.geo;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.geotools.geometry.jts.Geometries;
import org.geotools.geometry.jts.GeometryBuilder;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.GeodeticCalculator;
import org.opengis.geometry.BoundingBox;

import com.google.common.base.Preconditions;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.geom.PrecisionModel;
import com.vividsolutions.jts.io.InputStreamInStream;
import com.vividsolutions.jts.io.OutputStreamOutStream;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKBWriter;
import com.vividsolutions.jts.io.WKTReader;
import com.vividsolutions.jts.io.WKTWriter;
import com.vividsolutions.jts.precision.GeometryPrecisionReducer;

import marmot.Column;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSetException;
import marmot.type.GeometryDataType;
import utils.Size2d;
import utils.Size2i;
import utils.Utilities;
import utils.func.FOption;
import utils.stream.FStream;
import utils.stream.KVFStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GeoClientUtils {
	private static final ThreadLocal<GeodeticCalculator> GEODETIC_CALC
											= new ThreadLocal<GeodeticCalculator>();
	
	public final static GeometryFactory GEOM_FACT = new GeometryFactory();
	public final static Point EMPTY_POINT = GEOM_FACT.createPoint((Coordinate)null);
	public final static MultiPoint EMPTY_MULTIPOINT = GEOM_FACT.createMultiPoint((Coordinate[])null);
	public final static LineString EMPTY_LINESTRING = GEOM_FACT.createLineString(new Coordinate[0]);
	public final static LinearRing EMPTY_LINEARRING = GEOM_FACT.createLinearRing(new Coordinate[0]);
	public final static MultiLineString EMPTY_MULTILINESTRING = GEOM_FACT.createMultiLineString(null);
	public final static Polygon EMPTY_POLYGON = GEOM_FACT.createPolygon(new Coordinate[0]);
	public final static MultiPolygon EMPTY_MULTIPOLYGON = GEOM_FACT.createMultiPolygon(null);
	public final static GeometryCollection EMPTY_GEOM_COLLECTION = GEOM_FACT.createGeometryCollection(new Geometry[0]);
	public final static Geometry EMPTY_GEOMETRY = GEOM_FACT.createGeometry(EMPTY_POINT);
	
	public static Geometry emptyGeometry(Geometries type) {
		switch ( type) {
			case POINT:
				return EMPTY_POINT;
			case MULTIPOINT:
				return EMPTY_MULTIPOINT;
			case LINESTRING:
				return EMPTY_LINESTRING;
			case MULTILINESTRING:
				return EMPTY_MULTILINESTRING;
			case MULTIPOLYGON:
				return EMPTY_MULTIPOLYGON;
			case POLYGON:
				return EMPTY_POLYGON;
			case GEOMETRYCOLLECTION:
				return EMPTY_GEOM_COLLECTION;
			case GEOMETRY:
				return EMPTY_GEOMETRY;
			default:
				throw new AssertionError();
		}
	}
	
	final static GeometryBuilder GEOM_BUILDER = new GeometryBuilder(GEOM_FACT);
	
	public static double distanceWgs84(Point pt1, Point pt2) {
		GeodeticCalculator gc = GEODETIC_CALC.get();
		if ( gc == null ) {
			GEODETIC_CALC.set(gc = new GeodeticCalculator());
		}
		
		gc.setStartingGeographicPoint(pt1.getX(), pt1.getY());
		gc.setDestinationGeographicPoint(pt2.getX(), pt2.getY());
		return gc.getOrthodromicDistance();
	}
	
	public static Size2d size(Envelope envl) {
		return new Size2d(envl.getWidth(), envl.getHeight());
	}

	private static final String FPN = "(\\d+(\\.\\d*)?|(\\d+)?\\.\\d+)";
	private static final String PT = String.format("\\(\\s*%s\\s*,\\s*%s\\s*\\)", FPN, FPN);
	private static final String SZ = String.format("\\s*%s\\s*[xX]\\s*%s", FPN, FPN);
	private static final String ENVL =  String.format("\\s*%s\\s*:\\s*%s", PT, SZ);
	private static final Pattern PATTERN_ENVL = Pattern.compile(ENVL);
	public static FOption<Envelope> parseEnvelope(String expr) {
		Matcher matcher = PATTERN_ENVL.matcher(expr);
		if ( matcher.find() ) {
			FOption.empty();
		}
		
		double x = Double.parseDouble(matcher.group(1));
		double y = Double.parseDouble(matcher.group(4));
		Coordinate min = new Coordinate(x, y);
		
		double width = Double.parseDouble(matcher.group(7));
		double height = Double.parseDouble(matcher.group(10));
		Coordinate max = new Coordinate(x + width, y + height);
		
		return FOption.of(new Envelope(min, max));
	}
	
	public static String toString(Envelope envl) {
		double width = envl.getMaxX() - envl.getMinX();
		double height = envl.getMaxY() - envl.getMinY();
		return String.format("(%f,%f):%fx%f", envl.getMinX(), envl.getMinY(), width, height);
	}

	public static Geometry fromWKT(String wktStr) throws ParseException {
		return (wktStr != null) ? new WKTReader(GEOM_FACT).read(wktStr) : null;
	}

	public static String toWKT(Geometry geom) {
		return (geom != null) ? new WKTWriter().write(geom) : null;
	}

	public static Geometry fromWKB(byte[] wkbBytes) throws ParseException {
		return (wkbBytes != null) ? new WKBReader(GEOM_FACT).read(wkbBytes) : null;
	}
	
	public static Geometry fromWKB(InputStream is) throws ParseException, IOException {
		if ( is == null ) {
			return null;
		}
		else {
			return new WKBReader(GEOM_FACT).read(new InputStreamInStream(is));
		}
	}

	public static byte[] toWKB(Geometry geom) {
		return (geom != null && !geom.isEmpty()) ? new WKBWriter().write(geom) : null;
	}
	
	public static void toWKBStream(Geometry geom, OutputStream os) throws IOException {
		Preconditions.checkArgument(geom != null && !geom.isEmpty());
		
		new WKBWriter().write(geom, new OutputStreamOutStream(os));
	}

	public static Envelope toEnvelope(double tlX, double tlY, double brX, double brY) {
		Coordinate topLeft = new Coordinate(tlX, tlY);
		Coordinate bottomRight = new Coordinate(brX, brY);
		return new Envelope(topLeft, bottomRight);
	}

	public static Envelope toEnvelope(Coordinate tl, Coordinate br) {
		return new Envelope(tl, br);
	}

	public static Envelope toEnvelope(Point pt1, Point pt2) {
		return new Envelope(pt1.getCoordinate(), pt2.getCoordinate());
	}

	public static Envelope expandBy(Envelope envl, double distance) {
		Envelope expanded = new Envelope(envl);
		expanded.expandBy(distance);
		return expanded;
	}
	
	public static Point toPoint(double x, double y) {
		return GEOM_FACT.createPoint(new Coordinate(x, y));
	}
	
	public static Point toPoint(Coordinate coord) {
		return GEOM_FACT.createPoint(coord);
	}
	
	public static LineString toLineString(Coordinate... coords) {
		return GEOM_FACT.createLineString(coords);
	}
	
	public static LineString toLineString(List<Coordinate> coords) {
		return GEOM_FACT.createLineString(coords.toArray(new Coordinate[coords.size()]));
	}
	
	public static LinearRing toLinearRing(List<Coordinate> coords) {
		return GEOM_FACT.createLinearRing(coords.toArray(new Coordinate[coords.size()]));
	}
	
	public static Polygon toPolygon(LinearRing shell, List<LinearRing> holes) {
		LinearRing[] arr = holes.toArray(new LinearRing[holes.size()]);
		return GEOM_FACT.createPolygon(shell, arr);
	}
	
	public static Polygon toPolygon(Envelope envl) {
		Coordinate[] coords = new Coordinate[] {
			new Coordinate(envl.getMinX(), envl.getMinY()),	
			new Coordinate(envl.getMaxX(), envl.getMinY()),	
			new Coordinate(envl.getMaxX(), envl.getMaxY()),	
			new Coordinate(envl.getMinX(), envl.getMaxY()),	
			new Coordinate(envl.getMinX(), envl.getMinY()),	
		};
		LinearRing shell = GEOM_FACT.createLinearRing(coords);
		return GEOM_FACT.createPolygon(shell);
	}
	
	public static Polygon toPolygon(BoundingBox bbox) {
		Coordinate[] coords = new Coordinate[] {
			new Coordinate(bbox.getMinX(), bbox.getMinY()),	
			new Coordinate(bbox.getMaxX(), bbox.getMinY()),	
			new Coordinate(bbox.getMaxX(), bbox.getMaxY()),	
			new Coordinate(bbox.getMinX(), bbox.getMaxY()),	
			new Coordinate(bbox.getMinX(), bbox.getMinY()),	
		};
		LinearRing shell = GEOM_FACT.createLinearRing(coords);
		return GEOM_FACT.createPolygon(shell);
	}

	public static final int DEFAULT_REDUCER_FACTOR = Integer.MIN_VALUE;
	public static final int NO_REDUCER_FACTOR = -1;
	private static GeometryPrecisionReducer DEFAULT_PRECISION_REDUCER
														= toGeometryPrecisionReducer(2);
	public static GeometryPrecisionReducer toGeometryPrecisionReducer(int reduceFactor) {
		if ( reduceFactor == NO_REDUCER_FACTOR ) {
			return null;
		}
		else if ( reduceFactor >= 0 ) {
			double scale = Math.pow(10, reduceFactor);
			return new GeometryPrecisionReducer(new PrecisionModel(scale));
		}
		else {
			return DEFAULT_PRECISION_REDUCER;
		}
	}
	
	public static GeometryPrecisionReducer getDefaultPrecisionReducer() {
		return DEFAULT_PRECISION_REDUCER;
	}
	
	public static void setDefaultPrecisionReducer(int reduceFactor) {
		if ( reduceFactor == NO_REDUCER_FACTOR ) {
			DEFAULT_PRECISION_REDUCER = null;
		}
		else if ( reduceFactor >= 0 ) {
			double scale = Math.pow(10, reduceFactor);
			DEFAULT_PRECISION_REDUCER = new GeometryPrecisionReducer(new PrecisionModel(scale));
		}
		else {
			throw new IllegalArgumentException("invalid precision reducer factor: " + reduceFactor);
		}
	}
	
	public static Geometry makeValid(Geometry geom) {
		if ( geom instanceof MultiPolygon ) {
			return toMultiPolygon(flatten(geom, Polygon.class)
									.flatMap(p -> FStream.from(JTS.makeValid(p, false))))
						.buffer(0);
		}
		else if ( geom instanceof Polygon ) {
			return toMultiPolygon(JTS.makeValid((Polygon)geom, false)).buffer(0);
		}
		else {
			throw new UnsupportedOperationException("cannot make valid this Geometry: geom=" + geom);
		}
	}
	
	public static Geometry cast(Geometry geom, GeometryDataType dstType) {
		Utilities.checkNotNullArgument(geom, "geom is null");
		Utilities.checkNotNullArgument(dstType, "dstType is null");
		
		return cast(geom, dstType.toGeometries());
	}
	
	public static Geometry cast(Geometry geom, Geometries dstType) {
		Utilities.checkNotNullArgument(geom, "geom is null");
		Utilities.checkNotNullArgument(dstType, "dstType is null");
		
		if ( Geometries.get(geom) == dstType || dstType == Geometries.GEOMETRY ) {
			return geom;
		}
		
		switch ( dstType ) {
			case MULTIPOLYGON:
				return GEOM_FACT.createMultiPolygon(flatten(geom, Polygon.class)
														.toArray(Polygon.class));
			case POLYGON:
				return flatten(geom, Polygon.class).next().getOrElse(EMPTY_POLYGON);
			case POINT:
				return flatten(geom, Point.class).next().getOrElse(EMPTY_POINT);
			case MULTIPOINT:
				return toMultiPoint(flatten(geom, Point.class)
										.toArray(Point.class));
			case LINESTRING:
				return flatten(geom, LineString.class).next().getOrElse(EMPTY_LINESTRING);
			case MULTILINESTRING:
				return GEOM_FACT.createMultiLineString(flatten(geom, LineString.class)
														.toArray(LineString.class));
			case GEOMETRYCOLLECTION:
				return GEOM_FACT.createGeometryCollection(flatten(geom)
														.toArray(Geometry.class));
			default:
				throw new AssertionError("unexpected target type: type=" + dstType);
		}
	}
	
	@SuppressWarnings("unchecked")
	public static <T extends Geometry> T cast(Geometry geom, Class<T> dstType) {
		Utilities.checkNotNullArgument(geom, "geom is null");
		Utilities.checkNotNullArgument(dstType, "dstType is null");
		
		if ( dstType.isInstance(geom) || dstType == Geometry.class ) {
			return (T)geom;
		}
		
		if ( MultiPolygon.class == dstType ) {
			return (T)GEOM_FACT.createMultiPolygon(flatten(geom, Polygon.class)
													.toArray(Polygon.class));
		}
		else if ( Polygon.class == dstType ) {
			return (T)flatten(geom, Polygon.class).next().getOrElse(EMPTY_POLYGON);
		}
		else if ( Point.class == dstType ) {
			return (T)flatten(geom, Point.class).next().getOrElse(EMPTY_POINT);
		}
		else if ( MultiPoint.class == dstType ) {
			return (T)toMultiPoint(flatten(geom, Point.class)
													.toArray(Point.class));
		}
		else if ( LineString.class == dstType ) {
			return (T)flatten(geom, LineString.class).next().getOrElse(EMPTY_LINESTRING);
		}
		else if ( MultiLineString.class == dstType ) {
			return (T)GEOM_FACT.createMultiLineString(flatten(geom, LineString.class)
													.toArray(LineString.class));
		}
		else if ( GeometryCollection.class == dstType ) {
			return (T)GEOM_FACT.createGeometryCollection(flatten(geom)
													.toArray(Geometry.class));
		}
		
		throw new AssertionError("unexpected target type: type=" + dstType);
	}
	
	public static MultiPolygon castToMultiPolygon(Geometry src) {
		switch ( Geometries.get(src) ) {
			case MULTIPOLYGON:
				return (MultiPolygon)src;
			case POLYGON:
				return toMultiPolygon((Polygon)src);
			case GEOMETRYCOLLECTION:
				return toMultiPolygon(flatten(src, Polygon.class));
			default:
				return EMPTY_MULTIPOLYGON;
		}
	}
	
	public static FStream<Geometry> flatten(Geometry geom) {
		if ( geom instanceof GeometryCollection ) {
			return FStream.<Geometry>from(new GeometryIterator<>((GeometryCollection)geom))
							.flatMap(GeoClientUtils::flatten);
		}
		else {
			return FStream.of(geom);
		}
	}
	
	public static <T extends Geometry> FStream<T> flatten(Geometry geom, Class<T> cls) {
		return flatten(geom).castSafely(cls);
	}
	
	public static MultiPoint toMultiPoint(Point... pts) {
		return GEOM_FACT.createMultiPoint(pts);
	}
	
	public static MultiPoint toMultiPoint(Collection<Point> pts) {
		return GEOM_FACT.createMultiPoint(pts.toArray(new Point[pts.size()]));
	}
	
	public static MultiLineString toMultiLineString(LineString... lines) {
		return GEOM_FACT.createMultiLineString(lines);
	}
	
	public static MultiLineString toMultiLineString(Collection<LineString> lines) {
		return GEOM_FACT.createMultiLineString(lines.toArray(new LineString[lines.size()]));
	}
	
	public static MultiPolygon toMultiPolygon(Polygon... polys) {
		return GEOM_FACT.createMultiPolygon(polys);
	}
	
	public static MultiPolygon toMultiPolygon(List<Polygon> polyList) {
		return GEOM_FACT.createMultiPolygon(polyList.toArray(new Polygon[polyList.size()]));
	}
	
	public static MultiPolygon toMultiPolygon(FStream<Polygon> polygons) {
		return GEOM_FACT.createMultiPolygon(polygons.toArray(Polygon.class));
	}
	
	public static List<Polygon> getComponents(MultiPolygon mpoly) {
		return fstream(mpoly)
					.cast(Polygon.class)
					.toList();
	}
	
	public static <T extends Geometry> Iterator<T> components(GeometryCollection geom) {
		return new GeometryIterator<>(geom);
	}
	
	public static Stream<Geometry> componentStream(GeometryCollection geom) {
		return Utilities.stream(new GeometryIterator<>(geom));
	}
	
	public static FStream<Geometry> fstream(GeometryCollection geom) {
		return FStream.from(new GeometryIterator<>(geom));
	}

	private static final String DEFAULT_GEOM_COLUMN = "the_geom";
	public static Column findDefaultGeometryColumn(RecordSchema schema) {
		List<Column> geomColList = schema.streamColumns()
										.filter(col -> col.type().isGeometryType())
										.toList();
		if ( geomColList.size() == 1 ) {
			return geomColList.get(0);
		}
		else if ( geomColList.size() == 0 ) {
			throw new RecordSetException("No Geometry column in the RecordSet");
		}
		else { // if ( geomColList.size() > 1 ) {
			Column defCol = geomColList.stream()
										.filter(col -> col.matches(DEFAULT_GEOM_COLUMN))
										.findAny()
										.orElse(null);
			if ( defCol != null ) {
				return defCol;
			}
			
			throw new RecordSetException("Geometry column is not specified, "
										+ "but RecordSet has multiple Geometry columns");
		}
	}
	
	public static String toNonGeomString(Record record) {
		return KVFStream.from(record.toMap())
						.filterValue(v -> !(v instanceof Geometry))
						.toMap()
						.toString();
	}
	
	public static Size2d divide(Envelope envl, Size2i unit) {
		double xcount = envl.getWidth() / unit.getWidth();
		double ycount = envl.getHeight() / unit.getHeight();
		
		return new Size2d(xcount, ycount);
	}
	
	public static Size2d divide(Envelope envl, Size2d unit) {
		double xcount = envl.getWidth() / unit.getWidth();
		double ycount = envl.getHeight() / unit.getHeight();
		
		return new Size2d(xcount, ycount);
	}
}
