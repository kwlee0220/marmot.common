package marmot.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.geotools.geometry.jts.Geometries;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Point;

import marmot.geo.GeoClientUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PointType extends GeometryDataType {
	private static final PointType TYPE = new PointType();
	
	public static PointType get() {
		return TYPE;
	}
	
	private PointType() {
		super("point", TypeCode.POINT, Point.class);
	}
	
	@Override
	public Geometries toGeometries() {
		return Geometries.POINT;
	}

	@Override
	public Point newInstance() {
		return GeoClientUtils.EMPTY_POINT;
	}

	@Override
	protected Point readValidObject(DataInput in) throws IOException {
		double x = in.readDouble();
		double y = in.readDouble();
		return GeoClientUtils.toPoint(x, y);
	}

	@Override
	protected void writeValidObject(Object obj, DataOutput out) throws IOException {
		Coordinate coord = ((Point)obj).getCoordinate();
		
		out.writeDouble(coord.x);
		out.writeDouble(coord.y);
	}
}
