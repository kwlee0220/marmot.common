package marmot.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.io.ParseException;

import marmot.RecordSetException;
import marmot.geo.GeoClientUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class EnvelopeType extends DataType {
	public static final int NBYTES = 4 * 8;	// four doubles
	private static final EnvelopeType TYPE = new EnvelopeType();
	
	public static EnvelopeType get() {
		return TYPE;
	}
	
	private EnvelopeType() {
		super("envelope", TypeCode.ENVELOPE, Envelope.class);
	}

	@Override
	public Envelope newInstance() {
		return new Envelope();
	}
	
	@Override
	public Envelope fromString(String str) {
		try {
			return GeoClientUtils.fromWKT(str).getEnvelopeInternal();
		}
		catch ( ParseException e ) {
			throw new RecordSetException(e);
		}
	}
	
	@Override
	public String toString(Object instance) {
		return GeoClientUtils.toWKT(GeoClientUtils.toPolygon((Envelope)instance));
	}
	
	@Override
	public Envelope readObject(DataInput in) throws IOException {
		double p0x = in.readDouble();
		double p0y = in.readDouble();
		double p1x = in.readDouble();
		double p1y = in.readDouble();
		
		return new Envelope(new Coordinate(p0x, p0y), new Coordinate(p1x, p1y));
	}

	@Override
	public void writeObject(Object obj, DataOutput out) throws IOException {
		Envelope envl = (Envelope)obj;
		
		out.writeDouble(envl.getMinX());
		out.writeDouble(envl.getMinY());
		out.writeDouble(envl.getMaxX());
		out.writeDouble(envl.getMaxY());
	}
}
