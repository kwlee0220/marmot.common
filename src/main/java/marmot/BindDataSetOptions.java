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
							= new BindDataSetOptions(FOption.empty(), false);
	
	private final FOption<GeometryColumnInfo> m_gcInfo;
	private final boolean m_force;
	
	private BindDataSetOptions(FOption<GeometryColumnInfo> gcInfo, boolean force) {
		m_gcInfo = gcInfo;
		m_force = force;
	}
	
	public static BindDataSetOptions DEFAULT() {
		return new BindDataSetOptions(FOption.empty(), false);
	}
	
	public static BindDataSetOptions DEFAULT(GeometryColumnInfo gcInfo) {
		return new BindDataSetOptions(FOption.of(gcInfo), false);
	}
	
	public static BindDataSetOptions FORCE(boolean flag) {
		return new BindDataSetOptions(FOption.empty(), flag);
	}
	
	public static BindDataSetOptions FORCE(GeometryColumnInfo gcInfo) {
		return new BindDataSetOptions(FOption.of(gcInfo), true);
	}
	
	public FOption<GeometryColumnInfo> geometryColumnInfo() {
		return m_gcInfo;
	}
	
	public BindDataSetOptions geometryColumnInfo(GeometryColumnInfo gcInfo) {
		return new BindDataSetOptions(FOption.of(gcInfo), m_force);
	}
	
	public boolean force() {
		return m_force;
	}
	
	public BindDataSetOptions force(Boolean flag) {
		return new BindDataSetOptions(m_gcInfo, flag);
	}
	
	public StoreDataSetOptions toStoreDataSetOptions() {
		StoreDataSetOptions opts = StoreDataSetOptions.DEFAULT.force(m_force);
		opts = m_gcInfo.transform(opts, StoreDataSetOptions::geometryColumnInfo);
		
		return opts;
	}

	public static BindDataSetOptions fromProto(BindDataSetOptionsProto proto) {
		BindDataSetOptions options = BindDataSetOptions.EMPTY
														.force(proto.getForce());
		
		switch ( proto.getOptionalGeomColInfoCase() ) {
			case GEOM_COL_INFO:
				GeometryColumnInfo gcInfo = GeometryColumnInfo.fromProto(proto.getGeomColInfo());
				options = options.geometryColumnInfo(gcInfo);
				break;
			default:
		}
		
		return options;
	}
	
	public BindDataSetOptionsProto toProto() {
		BindDataSetOptionsProto.Builder builder = BindDataSetOptionsProto.newBuilder()
																		.setForce(m_force);
		m_gcInfo.map(GeometryColumnInfo::toProto).ifPresent(builder::setGeomColInfo);
		
		return builder.build();
	}

}
