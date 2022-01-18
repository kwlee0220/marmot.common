package marmot.type;

import org.geotools.geometry.jts.Geometries;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;

import com.google.common.base.Preconditions;

import marmot.geo.GeoClientUtils;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class GeometryDataType extends DataType {
	public abstract Geometries toGeometries();
//	protected abstract Geometry readValidObject(DataInput in) throws IOException;
//	protected abstract void writeValidObject(Object obj, DataOutput out) throws IOException;
	
	protected GeometryDataType(String name, TypeCode tc, Class<?> instClass) {
		super(name, tc, instClass);
	}
	
	public abstract Geometry newInstance();
	
	public final GeometryDataType pluralType() {
		switch ( getTypeCode() ) {
			case POINT:
			case MULTI_POINT:
				return DataType.MULTI_POINT;
			case LINESTRING:
			case MULTI_LINESTRING:
				return DataType.MULTI_LINESTRING;
			case POLYGON:
			case MULTI_POLYGON:
				return DataType.MULTI_POLYGON;
			case GEOM_COLLECTION:
			case GEOMETRY:
				return DataType.GEOM_COLLECTION;
			default:
				throw new AssertionError();
		}
	}
	
	@Override
	public Geometry parseInstance(String str) {
		Preconditions.checkArgument(str != null, "input WKT is null");
		
		try {
			return (str.length() > 0) ? (Geometry)GeoClientUtils.fromWKT(str) : null;
		}
		catch ( ParseException e ) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public String toInstanceString(Object geom) {
		Preconditions.checkArgument(geom != null, "input Geometry is null");
		Preconditions.checkArgument(geom instanceof Geometry, "input is not Geometry");
		
		return GeoClientUtils.toWKT((Geometry)geom);
	}
	
	public static GeometryDataType fromGeometries(Geometries geoms) {
		switch ( geoms ) {
			case POINT:
				return DataType.POINT;
			case POLYGON:
				return DataType.POLYGON;
			case MULTIPOLYGON:
				return DataType.MULTI_POLYGON;
			case MULTIPOINT:
				return DataType.MULTI_POINT;
			case LINESTRING:
				return DataType.LINESTRING;
			case MULTILINESTRING:
				return DataType.MULTI_LINESTRING;
			case GEOMETRYCOLLECTION:
				return DataType.GEOM_COLLECTION;
			case GEOMETRY:
				return DataType.GEOMETRY;
			default:
				throw new AssertionError("unexpected Geometries: " + geoms);
		}
	}
	
	public static GeometryDataType fromGeometry(Geometry geom) {
		return fromGeometries(Geometries.get(geom));
	}
}
