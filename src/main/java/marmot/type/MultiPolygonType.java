package marmot.type;

import org.geotools.geometry.jts.Geometries;
import org.locationtech.jts.geom.MultiPolygon;

import marmot.geo.GeoClientUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MultiPolygonType extends GeometryDataType {
	private static final MultiPolygonType TYPE = new MultiPolygonType();
	
	public static MultiPolygonType get() {
		return TYPE;
	}
	
	private MultiPolygonType() {
		super("multi_polygon", TypeCode.MULTI_POLYGON, MultiPolygon.class);
	}

	@Override
	public Geometries toGeometries() {
		return Geometries.MULTIPOLYGON;
	}

	@Override
	public MultiPolygon newInstance() {
		return GeoClientUtils.EMPTY_MULTIPOLYGON;
	}
}
