package marmot.plan;

import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;

import marmot.proto.optor.GeomOpOptionsProto;
import marmot.support.PBSerializable;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GeomOpOptions implements PBSerializable<GeomOpOptionsProto>, Serializable {
	private static final long serialVersionUID = 1L;

	public static final GeomOpOptions DEFAULT = new GeomOpOptions(FOption.empty(), FOption.empty());
	
	private FOption<String> m_outColumn = FOption.empty();
	private FOption<Boolean> m_throwOpError = FOption.empty();
	
	private GeomOpOptions(FOption<String> outputColumns, FOption<Boolean> throwOpError) {
		m_outColumn = outputColumns;
		m_throwOpError = throwOpError;
	}
	
	public static GeomOpOptions OUTPUT(String outCol) {
		return new GeomOpOptions(FOption.of(outCol), FOption.empty());
	}
	
	public FOption<String> outputColumn() {
		return m_outColumn;
	}
	
	public GeomOpOptions outputColumn(String outCol) {
		return new GeomOpOptions(FOption.of(outCol), m_throwOpError);
	}
	
	public FOption<Boolean> throwOpError() {
		return m_throwOpError;
	}
	
	public GeomOpOptions throwOpError(boolean flag) {
		return new GeomOpOptions(m_outColumn, FOption.of(flag));
	}

	public static GeomOpOptions fromProto(GeomOpOptionsProto proto) {
		return GeomOpOptions.DEFAULT.loadFromProto(proto);
	}

	public GeomOpOptions loadFromProto(GeomOpOptionsProto proto) {
		GeomOpOptions opts = this;
		switch ( proto.getOptionalOutGeomColCase() ) {
			case OUT_GEOM_COL:
				opts = opts.outputColumn(proto.getOutGeomCol());
				break;
			default:
		}
		switch ( proto.getOptionalThrowOpErrorCase() ) {
			case THROW_OP_ERROR:
				opts = opts.throwOpError(proto.getThrowOpError());
				break;
			default:
		}
		
		return opts;
	}

	@Override
	public GeomOpOptionsProto toProto() {
		GeomOpOptionsProto.Builder builder = GeomOpOptionsProto.newBuilder();
		
		m_outColumn.ifPresent(builder::setOutGeomCol);
		m_throwOpError.ifPresent(builder::setThrowOpError);
		
		return builder.build();
	}
	
	private Object writeReplace() {
		return new SerializationProxy(this);
	}
	
	private void readObject(ObjectInputStream stream) throws InvalidObjectException {
		throw new InvalidObjectException("Use Serialization Proxy instead.");
	}

	private static class SerializationProxy implements Serializable {
		private static final long serialVersionUID = 1L;
		
		private final GeomOpOptionsProto m_proto;
		
		private SerializationProxy(GeomOpOptions opts) {
			m_proto = opts.toProto();
		}
		
		private Object readResolve() {
			return GeomOpOptions.fromProto(m_proto);
		}
	}
}
