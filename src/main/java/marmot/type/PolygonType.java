package marmot.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.geotools.geometry.jts.Geometries;

import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Polygon;

import marmot.geo.GeoClientUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PolygonType extends GeometryDataType {
	private static final PolygonType TYPE = new PolygonType();
	
	public static PolygonType get() {
		return TYPE;
	}
	
	private PolygonType() {
		super("polygon", TypeCode.POLYGON, Polygon.class);
	}
	
	@Override
	public Geometries toGeometries() {
		return Geometries.POLYGON;
	}

	@Override
	public Polygon newInstance() {
		return GeoClientUtils.EMPTY_POLYGON;
	}

	@Override
	protected Polygon readValidObject(DataInput in) throws IOException {
		LinearRing shell = DataType.LINESTRING.readValidLinearRing(in);
		
		int nholes = in.readInt();
		LinearRing[] holes = new LinearRing[nholes];
		for ( int i =0; i < nholes; ++i ) {
			holes[i] = DataType.LINESTRING.readValidLinearRing(in);
		}
		
		return GeoClientUtils.GEOM_FACT.createPolygon(shell, holes);
	}

	@Override
	protected void writeValidObject(Object obj, DataOutput out) throws IOException {
		Polygon poly = (Polygon)obj;
		
		DataType.LINESTRING.writeValidObject(poly.getExteriorRing(), out);
		
		int ngeoms = poly.getNumInteriorRing();
		out.writeInt(ngeoms);
		for ( int i =0; i < ngeoms; ++i ) {
			DataType.LINESTRING.writeValidObject(poly.getInteriorRingN(i), out);
		}
	}
}
