package marmot.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.geotools.geometry.jts.Geometries;

import com.google.common.base.Preconditions;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;

import marmot.geo.GeoClientUtils;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class GeometryDataType extends DataType {
	public abstract Geometries toGeometries();
	protected abstract Geometry readValidObject(DataInput in) throws IOException;
	protected abstract void writeValidObject(Object obj, DataOutput out) throws IOException;
	
	protected GeometryDataType(String name, TypeCode tc, Class<?> instClass) {
		super(name, tc, instClass);
	}
	
	public abstract Geometry newInstance();
	
	public final GeometryDataType pluralType() {
		switch ( getTypeCode() ) {
			case POINT:
				return DataType.MULTI_POINT;
			case LINESTRING:
				return DataType.MULTI_LINESTRING;
			case POLYGON:
				return DataType.MULTI_POLYGON;
			case MULTI_POINT:
			case MULTI_LINESTRING:
			case GEOM_COLLECTION:
			case MULTI_POLYGON:
				return DataType.GEOM_COLLECTION;
			case GEOMETRY:
				return DataType.GEOM_COLLECTION;
			default:
				throw new AssertionError();
		}
	}
	
	@Override
	public Geometry fromString(String str) {
		Preconditions.checkArgument(str != null, "input WKT is null");
		
		try {
			return (str.length() > 0) ? (Geometry)GeoClientUtils.fromWKT(str) : null;
		}
		catch ( ParseException e ) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public String toString(Object geom) {
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
	
	public static GeometryDataType fromGeometries(Geometry geom) {
		return fromGeometries(Geometries.get(geom));
	}
	
	static Coordinate[] readCoordinates(DataInput in) throws IOException {
		int ncoords = in.readInt();
		
		Coordinate[] coords = new Coordinate[ncoords];
		for ( int i =0; i < ncoords; ++i ) {
			double x = in.readDouble();
			double y = in.readDouble();
			
			coords[i] = new Coordinate(x, y);
		}
		
		return coords;
	}
	
	static void writeCoordinates(Coordinate[] coords, DataOutput out) throws IOException {
		out.writeInt(coords.length);
		for ( Coordinate coord: coords ) {
			out.writeDouble(coord.x);
			out.writeDouble(coord.y);
		}
	}
}
