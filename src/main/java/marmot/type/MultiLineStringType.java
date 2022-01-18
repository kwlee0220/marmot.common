package marmot.type;

import org.geotools.geometry.jts.Geometries;
import org.locationtech.jts.geom.MultiLineString;

import marmot.geo.GeoClientUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MultiLineStringType extends GeometryDataType {
	private static final MultiLineStringType TYPE = new MultiLineStringType();
	
	public static MultiLineStringType get() {
		return TYPE;
	}
	
	private MultiLineStringType() {
		super("multi_line_string", TypeCode.MULTI_LINESTRING, MultiLineString.class);
	}
	
	@Override
	public Geometries toGeometries() {
		return Geometries.MULTILINESTRING;
	}

	@Override
	public MultiLineString newInstance() {
		return GeoClientUtils.EMPTY_MULTILINESTRING;
	}
}
