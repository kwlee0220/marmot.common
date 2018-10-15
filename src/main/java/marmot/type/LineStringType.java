package marmot.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.geotools.geometry.jts.Geometries;

import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.LinearRing;

import marmot.geo.GeoClientUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class LineStringType extends GeometryDataType {
	private static final LineStringType TYPE = new LineStringType();
	
	public static LineStringType get() {
		return TYPE;
	}
	
	private LineStringType() {
		super("line_string", TypeCode.LINESTRING, LineString.class);
	}
	
	@Override
	public Geometries toGeometries() {
		return Geometries.LINESTRING;
	}

	@Override
	public LineString newInstance() {
		return GeoClientUtils.EMPTY_LINESTRING;
	}

	@Override
	protected LineString readValidObject(DataInput in) throws IOException {
		return GeoClientUtils.GEOM_FACT.createLineString(readCoordinates(in));
	}
	
	LinearRing readValidLinearRing(DataInput in) throws IOException {
		return GeoClientUtils.GEOM_FACT.createLinearRing(readCoordinates(in));
	}

	@Override
	protected void writeValidObject(Object obj, DataOutput out) throws IOException {
		writeCoordinates(((LineString)obj).getCoordinates(), out);
	}
}
