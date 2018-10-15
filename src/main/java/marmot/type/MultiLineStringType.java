package marmot.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.geotools.geometry.jts.Geometries;

import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiLineString;

import marmot.geo.GeoClientUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MultiLineStringType extends GeometryDataType {
	private static final MultiLineStringType TYPE = new MultiLineStringType();
	
	public static MultiLineStringType get() {
		return TYPE;
	}
	
	private MultiLineStringType() {
		super("multi_line_string", TypeCode.MULTI_LINESTRING, MultiLineString.class);
	}
	
	@Override
	public Geometries toGeometries() {
		return Geometries.MULTILINESTRING;
	}

	@Override
	public MultiLineString newInstance() {
		return GeoClientUtils.EMPTY_MULTILINESTRING;
	}

	@Override
	protected MultiLineString readValidObject(DataInput in) throws IOException {
		int nlines = in.readInt();
		LineString[] lines = new LineString[nlines];
		for ( int i =0; i < nlines; ++i ) {
			lines[i] = DataType.LINESTRING.readValidObject(in);
		}
		
		return GeoClientUtils.GEOM_FACT.createMultiLineString(lines);
	}

	@Override
	protected void writeValidObject(Object obj, DataOutput out) throws IOException {
		MultiLineString mline = (MultiLineString)obj;
		
		int ngeoms = mline.getNumGeometries();
		out.writeInt(ngeoms);
		for ( int i =0; i < ngeoms; ++i ) {
			DataType.LINESTRING.writeValidObject(mline.getGeometryN(i), out);
		}
	}
}
