package marmot.plan;

import marmot.proto.optor.GeomOpOptionsProto;
import marmot.support.PBSerializable;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GeomOpOptions implements PBSerializable<GeomOpOptionsProto> {
	private FOption<String> m_outColumns = FOption.empty();
	private FOption<Boolean> m_skipError = FOption.empty();
	
	public static GeomOpOptions create() {
		return new GeomOpOptions();
	}
	
	public static GeomOpOptions OUTPUT(String outCol) {
		return create().outputColumn(outCol);
	}
	
	public FOption<String> outputColumns() {
		return m_outColumns;
	}
	
	public GeomOpOptions outputColumn(String outCol) {
		m_outColumns = FOption.ofNullable(outCol);
		return this;
	}
	
	public FOption<Boolean> skipError() {
		return m_skipError;
	}
	
	public GeomOpOptions skipError(boolean flag) {
		m_skipError = FOption.of(flag);
		return this;
	}

	public static GeomOpOptions fromProto(GeomOpOptionsProto proto) {
		GeomOpOptions opts = GeomOpOptions.create();
		
		switch ( proto.getOptionalOutGeomColCase() ) {
			case OUT_GEOM_COL:
				opts.outputColumn(proto.getOutGeomCol());
				break;
			default:
		}
		switch ( proto.getOptionalSkipErrorCase() ) {
			case SKIP_ERROR:
				opts.skipError(proto.getSkipError());
				break;
			default:
		}
		
		return opts;
	}

	@Override
	public GeomOpOptionsProto toProto() {
		GeomOpOptionsProto.Builder builder = GeomOpOptionsProto.newBuilder();
		
		m_outColumns.ifPresent(builder::setOutGeomCol);
		m_skipError.ifPresent(builder::setSkipError);
		
		return builder.build();
	}
}
