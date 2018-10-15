package marmot.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.geotools.geometry.jts.Geometries;

import com.vividsolutions.jts.geom.Geometry;
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

	@Override
	protected GeometryCollection readValidObject(DataInput in) throws IOException {
		int ngeoms = in.readInt();
		
		Geometry[] geoms = new Geometry[ngeoms];
		for ( int i =0; i < ngeoms; ++i ) {
			geoms[i] = DataType.GEOMETRY.readValidObject(in);
		}
		
		return GeoClientUtils.GEOM_FACT.createGeometryCollection(geoms);
	}

	@Override
	protected void writeValidObject(Object obj, DataOutput out) throws IOException {
		GeometryCollection coll = (GeometryCollection)obj;
		
		int ngeoms = coll.getNumGeometries();
		out.writeInt(ngeoms);
		for ( int i =0; i < ngeoms; ++i ) {
			DataType.GEOMETRY.writeValidObject(coll.getGeometryN(i), out);
		}
	}
}
