package marmot.type;

import org.geotools.geometry.jts.Geometries;
import org.locationtech.jts.geom.LineString;

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
}
