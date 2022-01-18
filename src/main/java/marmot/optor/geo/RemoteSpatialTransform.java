package marmot.optor.geo;

import org.locationtech.jts.geom.Geometry;

import com.google.common.base.Preconditions;

import marmot.type.DataType;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RemoteSpatialTransform {
	private final String m_code;
	private final String m_inGeomCol;
	private String m_outGeomCol;
	private final Object[] m_args;
	
	private RemoteSpatialTransform(String code, String inGeomCol, String outGeomCol,
									Object... args) {
		m_code = code;
		m_inGeomCol = inGeomCol;
		m_outGeomCol = outGeomCol;
		m_args = args;
	}
	
	public String code() {
		return (String)m_code;
	}
	
	public String inputGeometryColumn() {
		return m_inGeomCol;
	}
	
	public String outputGeometryColumn() {
		return m_outGeomCol;
	}
	
	public RemoteSpatialTransform outputGeometryColumn(String colName) {
		Preconditions.checkArgument(colName != null);
		
		m_outGeomCol = colName;
		return this;
	}
	
	public static RemoteSpatialTransform centroid(String geomCol) {
		return new RemoteSpatialTransform("centroid", geomCol, geomCol);
	}
	
	public static RemoteSpatialTransform buffer(String geomCol, double distance) {
		return new RemoteSpatialTransform("buffer", geomCol, geomCol, distance);
	}
	
	public static RemoteSpatialTransform intersection(String geomCol, Geometry param) {
		return new RemoteSpatialTransform("intersection_unary", geomCol, geomCol, param);
	}

	public static RemoteSpatialTransform intersection(String leftGeomCol, String rightGeomCol,
														DataType outputGeomType) {
		return new RemoteSpatialTransform("intersection_binary", leftGeomCol, leftGeomCol,
											rightGeomCol, outputGeomType);
	}
	
	public static RemoteSpatialTransform reduceGeometryPrecision(String geomCol, int reduceFactor) {
		return new RemoteSpatialTransform("reduce_geometry_precision", geomCol, geomCol,
											reduceFactor);
	}
	
	public static RemoteSpatialTransform transformCRS(String geomCol, String srcSrid,
														String tarSrid) {
		return new RemoteSpatialTransform("transform_crs", geomCol, geomCol, srcSrid, tarSrid);
	}
}
