package marmot.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.geotools.geometry.jts.Geometries;

import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Polygon;

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

	@Override
	protected MultiPolygon readValidObject(DataInput in) throws IOException {
		int npolys = in.readInt();
		
		Polygon[] polys = new Polygon[npolys];
		for ( int i =0; i < npolys; ++i ) {
			polys[i] = DataType.POLYGON.readValidObject(in);
		}
		
		return GeoClientUtils.GEOM_FACT.createMultiPolygon(polys);
	}

	@Override
	protected void writeValidObject(Object obj, DataOutput out) throws IOException {
		if ( obj instanceof Polygon ) {
			obj = GeoClientUtils.toMultiPolygon((Polygon)obj);
		}
		
		MultiPolygon mpoly = (MultiPolygon)obj;
		int ngeoms = mpoly.getNumGeometries();
		out.writeInt(ngeoms);
		for ( int i =0; i < ngeoms; ++i ) {
			DataType.POLYGON.writeValidObject(mpoly.getGeometryN(i), out);
		}
	}
}
