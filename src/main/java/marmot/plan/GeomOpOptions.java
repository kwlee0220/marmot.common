package marmot.plan;

import marmot.proto.optor.GeomOpOptionsProto;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GeomOpOptions {
	private FOption<String> m_outColumn = FOption.empty();
	private FOption<Boolean> m_throwOpError = FOption.empty();
	
	public static GeomOpOptions create() {
		return new GeomOpOptions();
	}
	
	public static GeomOpOptions OUTPUT(String outCol) {
		return new GeomOpOptions().outputColumn(outCol);
	}
	
	public FOption<String> outputColumn() {
		return m_outColumn;
	}
	
	public GeomOpOptions outputColumn(String outCol) {
		m_outColumn = FOption.ofNullable(outCol);
		return this;
	}
	
	public FOption<Boolean> throwOpError() {
		return m_throwOpError;
	}
	
	public GeomOpOptions throwOpError(boolean flag) {
		m_throwOpError = FOption.of(flag);
		return this;
	}

	public static GeomOpOptions fromProto(GeomOpOptionsProto proto) {
		return GeomOpOptions.create().loadFromProto(proto);
	}

	public GeomOpOptions loadFromProto(GeomOpOptionsProto proto) {
		switch ( proto.getOptionalOutGeomColCase() ) {
			case OUT_GEOM_COL:
				outputColumn(proto.getOutGeomCol());
				break;
			default:
		}
		switch ( proto.getOptionalThrowOpErrorCase() ) {
			case THROW_OP_ERROR:
				throwOpError(proto.getThrowOpError());
				break;
			default:
		}
		
		return this;
	}

	public GeomOpOptionsProto toProto() {
		GeomOpOptionsProto.Builder builder = GeomOpOptionsProto.newBuilder();
		
		m_outColumn.ifPresent(builder::setOutGeomCol);
		m_throwOpError.ifPresent(builder::setThrowOpError);
		
		return builder.build();
	}
}
