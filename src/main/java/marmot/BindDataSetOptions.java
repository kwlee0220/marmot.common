package marmot;

import marmot.proto.service.BindDataSetOptionsProto;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class BindDataSetOptions {
	private FOption<GeometryColumnInfo> m_gcInfo = FOption.empty();
	private FOption<Boolean> m_force = FOption.empty();
			
	public static BindDataSetOptions create() {
		return new BindDataSetOptions();
	}
	
	public static BindDataSetOptions DEFAULT() {
		return new BindDataSetOptions().force(true);
	}
	
	public static BindDataSetOptions DEFAULT(GeometryColumnInfo gcInfo) {
		return new BindDataSetOptions().geometryColumnInfo(gcInfo).force(true);
	}
	
	public FOption<GeometryColumnInfo> geometryColumnInfo() {
		return m_gcInfo;
	}
	
	public BindDataSetOptions geometryColumnInfo(GeometryColumnInfo gcInfo) {
		m_gcInfo = FOption.ofNullable(gcInfo);
		return this;
	}
	
	public FOption<Boolean> force() {
		return m_force;
	}
	
	public BindDataSetOptions force(Boolean flag) {
		m_force = FOption.ofNullable(flag);
		return this;
	}
	
	public BindDataSetOptions duplicate() {
		BindDataSetOptions opts = BindDataSetOptions.create();
		opts.m_gcInfo = m_gcInfo;
		opts.m_force = m_force;
		
		return opts;
	}
	
	public StoreDataSetOptions toStoreDataSetOptions() {
		StoreDataSetOptions opts = StoreDataSetOptions.create();
		m_gcInfo.ifPresent(opts::geometryColumnInfo);
		m_force.ifPresent(opts::force);
		
		return opts;
	}

	public static BindDataSetOptions fromProto(BindDataSetOptionsProto proto) {
		BindDataSetOptions options = BindDataSetOptions.create();
		
		switch ( proto.getOptionalGeomColInfoCase() ) {
			case GEOM_COL_INFO:
				options.geometryColumnInfo(GeometryColumnInfo.fromProto(proto.getGeomColInfo()));
				break;
			default:
		}
		switch ( proto.getOptionalForceCase() ) {
			case FORCE:
				options.force(proto.getForce());
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
