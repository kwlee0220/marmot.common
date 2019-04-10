package marmot.type;

import org.geotools.geometry.jts.Geometries;

import com.vividsolutions.jts.geom.GeometryCollection;

import marmot.geo.GeoClientUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GeometryCollectionType extends GeometryDataType {
	private static final GeometryCollectionType TYPE = new GeometryCollectionType();
	
	public static GeometryCollectionType get() {
		return TYPE;
	}
	
	private GeometryCollectionType() {
		super("geom_collection", TypeCode.GEOM_COLLECTION, GeometryCollection.class);
	}
	
	@Override
	public Geometries toGeometries() {
		return Geometries.GEOMETRYCOLLECTION;
	}

	@Override
	public GeometryCollection newInstance() {
		return GeoClientUtils.EMPTY_GEOM_COLLECTION;
	}
}
