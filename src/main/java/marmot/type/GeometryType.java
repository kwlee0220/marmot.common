package marmot.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.geotools.geometry.jts.Geometries;

import com.vividsolutions.jts.geom.Geometry;

import marmot.geo.GeoClientUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GeometryType extends GeometryDataType {
	private static final GeometryType TYPE = new GeometryType();
	
	public static GeometryType get() {
		return TYPE;
	}
	
	private GeometryType() {
		super("geometry", TypeCode.GEOMETRY, Geometry.class);
	}
	
	@Override
	public Geometries toGeometries() {
		return Geometries.GEOMETRY;
	}

	@Override
	public Geometry newInstance() {
		return GeoClientUtils.EMPTY_GEOMETRY;
	}

	@Override
	protected Geometry readValidObject(DataInput in) throws IOException {
		GeometryDataType type = (GeometryDataType)DataTypes.fromTypeCode(in.readByte());
		return type.readValidObject(in);
	}

	@Override
	protected void writeValidObject(Object obj, DataOutput out) throws IOException {
		Geometry geom = (Geometry)obj;
		GeometryDataType type = GeometryDataType.fromGeometries(Geometries.get(geom));
		
		out.writeByte(type.getTypeCode().ordinal());
		type.writeValidObject(obj, out);
	}
}
