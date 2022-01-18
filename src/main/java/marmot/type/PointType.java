package marmot.type;

import org.geotools.geometry.jts.Geometries;
import org.locationtech.jts.geom.Point;

import marmot.geo.GeoClientUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PointType extends GeometryDataType {
	private static final PointType TYPE = new PointType();
	
	public static PointType get() {
		return TYPE;
	}
	
	private PointType() {
		super("point", TypeCode.POINT, Point.class);
	}
	
	@Override
	public Geometries toGeometries() {
		return Geometries.POINT;
	}

	@Override
	public Point newInstance() {
		return GeoClientUtils.EMPTY_POINT;
	}
}
