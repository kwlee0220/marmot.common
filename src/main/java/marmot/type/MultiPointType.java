package marmot.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.geotools.geometry.jts.Geometries;

import com.vividsolutions.jts.geom.MultiPoint;

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

	@Override
	protected MultiPoint readValidObject(DataInput in) throws IOException {
		return GeoClientUtils.GEOM_FACT.createMultiPoint(readCoordinates(in));
	}

	@Override
	protected void writeValidObject(Object obj, DataOutput out) throws IOException {
		writeCoordinates(((MultiPoint)obj).getCoordinates(), out);
	}
}
