package marmot.type;

import org.geotools.geometry.jts.Geometries;
import org.locationtech.jts.geom.MultiPoint;

import marmot.geo.GeoClientUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MultiPointType extends GeometryDataType {
	private static final MultiPointType TYPE = new MultiPointType();
	
	public static MultiPointType get() {
		return TYPE;
	}
	
	private MultiPointType() {
		super("multi_point", TypeCode.MULTI_POINT, MultiPoint.class);
	}
	
	@Override
	public Geometries toGeometries() {
		return Geometries.MULTIPOINT;
	}

	@Override
	public MultiPoint newInstance() {
		return GeoClientUtils.EMPTY_MULTIPOINT;
	}
}
