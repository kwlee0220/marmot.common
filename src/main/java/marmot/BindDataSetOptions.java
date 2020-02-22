package marmot;

import marmot.dataset.GeometryColumnInfo;
import marmot.optor.StoreDataSetOptions;
import marmot.proto.service.BindDataSetOptionsProto;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class BindDataSetOptions {
	public static final BindDataSetOptions EMPTY
							= new BindDataSetOptions(FOption.empty(), FOption.empty());
	
	private final FOption<GeometryColumnInfo> m_gcInfo;
	private final FOption<Boolean> m_force;
	
	private BindDataSetOptions(FOption<GeometryColumnInfo> gcInfo, FOption<Boolean> force) {
		m_gcInfo = gcInfo;
		m_force = force;
	}
	
	public static BindDataSetOptions DEFAULT() {
		return new BindDataSetOptions(FOption.empty(), FOption.of(true));
	}
	
	public static BindDataSetOptions DEFAULT(GeometryColumnInfo gcInfo) {
		return new BindDataSetOptions(FOption.of(gcInfo), FOption.of(true));
	}
	
	public static BindDataSetOptions FORCE(boolean flag) {
		return new BindDataSetOptions(FOption.empty(), FOption.of(flag));
	}
	
	public FOption<GeometryColumnInfo> geometryColumnInfo() {
		return m_gcInfo;
	}
	
	public BindDataSetOptions geometryColumnInfo(GeometryColumnInfo gcInfo) {
		return new BindDataSetOptions(FOption.of(gcInfo), m_force);
	}
	
	public FOption<Boolean> force() {
		return m_force;
	}
	
	public BindDataSetOptions force(Boolean flag) {
		return new BindDataSetOptions(m_gcInfo, FOption.of(flag));
	}
	
	public StoreDataSetOptions toStoreDataSetOptions() {
		StoreDataSetOptions opts = StoreDataSetOptions.DEFAULT;
		opts = m_gcInfo.transform(opts, StoreDataSetOptions::geometryColumnInfo);
		opts = m_force.transform(opts, StoreDataSetOptions::force);
		
		return opts;
	}

	public static BindDataSetOptions fromProto(BindDataSetOptionsProto proto) {
		BindDataSetOptions options = BindDataSetOptions.EMPTY;
		
		switch ( proto.getOptionalGeomColInfoCase() ) {
			case GEOM_COL_INFO:
				GeometryColumnInfo gcInfo = GeometryColumnInfo.fromProto(proto.getGeomColInfo());
				options = options.geometryColumnInfo(gcInfo);
				break;
			default:
		}
		switch ( proto.getOptionalForceCase() ) {
			case FORCE:
				options = options.force(proto.getForce());
				break;
			default:
		}
		
		return options;
	}
	
	public BindDataSetOptionsProto toProto() {
		BindDataSetOptionsProto.Builder builder = BindDataSetOptionsProto.newBuilder();
		m_gcInfo.map(GeometryColumnInfo::toProto).ifPresent(builder::setGeomColInfo);
		m_force.map(builder::setForce);
		
		return builder.build();
	}

}
