package marmot.type;

import org.geotools.geometry.jts.Geometries;
import org.locationtech.jts.geom.Polygon;

import marmot.geo.GeoClientUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PolygonType extends GeometryDataType {
	private static final PolygonType TYPE = new PolygonType();
	
	public static PolygonType get() {
		return TYPE;
	}
	
	private PolygonType() {
		super("polygon", TypeCode.POLYGON, Polygon.class);
	}
	
	@Override
	public Geometries toGeometries() {
		return Geometries.POLYGON;
	}

	@Override
	public Polygon newInstance() {
		return GeoClientUtils.EMPTY_POLYGON;
	}
}
