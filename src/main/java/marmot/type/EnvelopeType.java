package marmot.type;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.io.ParseException;

import marmot.geo.GeoClientUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class EnvelopeType extends DataType {
	public static final Envelope NULL = null;
	public static final Envelope EMPTY = new Envelope();
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
	public Envelope parseInstance(String str) {
		try {
			return GeoClientUtils.fromWKT(str).getEnvelopeInternal();
		}
		catch ( ParseException e ) {
			throw new IllegalArgumentException(e);
		}
	}
	
	@Override
	public String toInstanceString(Object instance) {
		return GeoClientUtils.toWKT(GeoClientUtils.toPolygon((Envelope)instance));
	}
}
