package marmot.type;

import org.geotools.geometry.jts.Geometries;
import org.locationtech.jts.geom.Geometry;

import marmot.geo.GeoClientUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GeometryType extends GeometryDataType {
	private static final GeometryType TYPE = new GeometryType();
	
	public static GeometryType get() {
		return TYPE;
	}
	
	private GeometryType() {
		super("geometry", TypeCode.GEOMETRY, Geometry.class);
	}
	
	@Override
	public Geometries toGeometries() {
		return Geometries.GEOMETRY;
	}

	@Override
	public Geometry newInstance() {
		return GeoClientUtils.EMPTY_GEOMETRY;
	}
}
