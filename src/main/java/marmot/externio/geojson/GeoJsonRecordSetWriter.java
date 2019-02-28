package marmot.externio.geojson;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Objects;

import org.geotools.geometry.jts.Geometries;

import com.google.gson.stream.JsonWriter;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

import marmot.Column;
import marmot.DataSet;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.externio.RecordSetWriter;
import marmot.support.DefaultRecord;
import utils.UnitUtils;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GeoJsonRecordSetWriter implements RecordSetWriter {
	private static final int DEFAULT_BUFFER_SIZE = (int)UnitUtils.parseByteSize("64kb");
	
	private final BufferedWriter m_writer;
	private String m_geomCol;
	private boolean m_pretty = false;
	
	public static GeoJsonRecordSetWriter get(File path) throws IOException {
		BufferedWriter writer = Files.newBufferedWriter(path.toPath());
		return new GeoJsonRecordSetWriter(writer);
	}
	
	public static GeoJsonRecordSetWriter get(File path, Charset charset) throws IOException {
		BufferedWriter writer = Files.newBufferedWriter(path.toPath(), charset);
		return new GeoJsonRecordSetWriter(writer);
	}
	
	public static GeoJsonRecordSetWriter get(Writer writer) throws IOException {
		BufferedWriter bwriter = (writer instanceof BufferedWriter)
								? (BufferedWriter)writer
								: new BufferedWriter(writer, DEFAULT_BUFFER_SIZE);
		return new GeoJsonRecordSetWriter(bwriter);
	}

	public static GeoJsonRecordSetWriter get(OutputStream os,
											FOption<Charset> charset) throws IOException {
		Charset cs = charset.getOrElse(StandardCharsets.UTF_8);
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(os, cs), DEFAULT_BUFFER_SIZE);
		return new GeoJsonRecordSetWriter(writer);
	}
	
	private GeoJsonRecordSetWriter(BufferedWriter writer) {
		Objects.requireNonNull(writer, "BufferedWriter is null");
		
		m_writer = writer;
	}

	@Override
	public void close() throws IOException {
		m_writer.close();
	}
	
	public GeoJsonRecordSetWriter geometryColumn(String geomCol) {
		m_geomCol = geomCol;
		return this;
	}
	
	public GeoJsonRecordSetWriter prettyPrinter(boolean flag) {
		m_pretty = flag;
		return this;
	}

	@Override
	public long write(DataSet ds) throws IOException {
		if ( m_geomCol == null ) {
			m_geomCol = ds.getGeometryColumnInfo().name();
		}
		try ( RecordSet rset = ds.read() ) {
			return write(rset);
		}
	}

	@Override
	public long write(RecordSet rset) throws IOException {
		Objects.requireNonNull(m_geomCol, "Geometry column name is null");
		
		JsonWriter writer = new JsonWriter(m_writer);
		if ( m_pretty ) {
			writer.setIndent(" ");
		}
		
		long nrecs = 0;
		writer.beginObject();
			writer.name("type").value("FeatureCollection");
			writer.name("features");
			writer.beginArray();
				Record record = DefaultRecord.of(rset.getRecordSchema());
				while ( rset.next(record) ) {
					write(writer, record, m_geomCol);
					++nrecs;
				}
			writer.endArray();
		writer.endObject();
		writer.close();
		
		return nrecs;
	}
	
	private void write(JsonWriter writer, Record record, String geomColName)
		throws IOException {
		writer.beginObject();
		
		writer.name("type").value("Feature");
		writer.name("geometry");
		serializeGeometry(writer, record.getGeometry(geomColName));

		RecordSchema schema = record.getRecordSchema();
		if ( schema.getColumnCount() == 1 ) {
			return;
		}
		
		writer.name("properties");
		writer.beginObject();
		
		for ( int i = 0; i < schema.getColumnCount(); ++i ) {
			Column col = schema.getColumnAt(i);
			if ( col.matches(geomColName) ) {
				continue;
			}
			
			writer.name(col.name());
			
			Object field = record.get(i);
			if ( field != null ) {
				switch ( col.type().getTypeCode() ) {
					case STRING:
						writer.value((String)field);
						break;
					case DOUBLE:
						writer.value((double)field);
						break;
					case INT:
						writer.value((int)field);
						break;
					case LONG:
						writer.value((long)field);
						break;
					case BYTE:
						writer.value((byte)field);
						break;
					case FLOAT:
						writer.value((float)field);
						break;
					case SHORT:
						writer.value((short)field);
						break;
					case BOOLEAN:
						writer.value((boolean)field);
						break;
					default:
						writer.value(field.toString());
						break;
				}
			}
			else {
				writer.nullValue();
			}
		}
		writer.endObject();
		writer.endObject();
	}
	
	private void serializeGeometry(JsonWriter writer, Geometry geom) throws IOException {
		writer.beginObject();
		
		Geometries type = Geometries.get(geom);
		if ( type == Geometries.POLYGON ) {
			Polygon poly = (Polygon)geom;

			writer.name("type").value("Polygon");
			writer.name("coordinates");
			serializePolygon(writer, poly);
		}
		else if ( type == Geometries.POINT ) {
			Point pt = (Point)geom;

			writer.name("type").value("Point");
			writer.name("coordinates");
			serializePoint(writer, pt);
		}
		else if ( type == Geometries.MULTIPOLYGON ) {
			MultiPolygon mpoly = (MultiPolygon)geom;

			writer.name("type").value("MultiPolygon");
			writer.name("coordinates");
			serializeMultiPolygon(writer, mpoly);
		}
		else if ( type == Geometries.MULTIPOINT ) {
			MultiPoint mpoints = (MultiPoint)geom;

			writer.name("type").value("MultiPoint");
			writer.name("coordinates");
			serializeMultiPoint(writer, mpoints);
		}
		else if ( type == Geometries.LINESTRING ) {
			LineString line = (LineString)geom;

			writer.name("type").value("LineString");
			writer.name("coordinates");
			serializeLineString(writer, line);
		}
		else if ( type == Geometries.MULTILINESTRING ) {
			MultiLineString mlines = (MultiLineString)geom;

			writer.name("type").value("MultiLineString");
			writer.name("coordinates");
			serializeMultiLineString(writer, mlines);
		}
		else if ( type == Geometries.GEOMETRYCOLLECTION ) {
			GeometryCollection geomColl = (GeometryCollection)geom;

			writer.name("type").value("GeometryCollection");
			writer.name("coordinates");
			writer.beginArray();
				for ( int i =0; i < geomColl.getNumGeometries(); ++i ) {
					Geometry subGeom = geomColl.getGeometryN(i);
					serializeGeometry(writer, subGeom);
				}
			writer.endArray();
		}
		
		writer.endObject();
	}
	
	private void serializePoint(JsonWriter writer, Point pt) throws IOException {
		Coordinate coord = pt.getCoordinate();
		
		writer.beginArray();
		writer.value(coord.x);
		writer.value(coord.y);
		writer.endArray();
	}
	
	private void serializeLineString(JsonWriter writer, LineString line) throws IOException {
		writer.beginArray();
		for ( int i =0; i < line.getNumPoints(); ++i ) {
			Point pt = line.getPointN(i);
			serializePoint(writer, pt);
		}
		writer.endArray();
	}
	
	private void serializePolygon(JsonWriter writer, Polygon poly) throws IOException {
		writer.beginArray();
		serializeLineString(writer, poly.getExteriorRing());
		for ( int i =0; i < poly.getNumInteriorRing(); ++i ) {
			LineString line = poly.getInteriorRingN(i);
			serializeLineString(writer, line);
		}
		writer.endArray();
	}
	
	private void serializeMultiPolygon(JsonWriter writer, MultiPolygon mpoly) throws IOException {
		writer.beginArray();
		for ( int i =0; i < mpoly.getNumGeometries(); ++i ) {
			Polygon poly = (Polygon)mpoly.getGeometryN(i);
			serializePolygon(writer, poly);
		}
		writer.endArray();
	}
	
	private void serializeMultiPoint(JsonWriter jgen, MultiPoint mpoints) throws IOException {
		jgen.beginArray();
		for ( int i =0; i < mpoints.getNumGeometries(); ++i ) {
			Point pt = (Point)mpoints.getGeometryN(i);
			serializePoint(jgen, pt);
		}
		jgen.endArray();
	}
	
	private void serializeMultiLineString(JsonWriter jgen, MultiLineString mlines)
		throws IOException {
		jgen.beginArray();
		for ( int i =0; i < mlines.getNumGeometries(); ++i ) {
			LineString pt = (LineString)mlines.getGeometryN(i);
			serializeLineString(jgen, pt);
		}
		jgen.endArray();
	}
}
