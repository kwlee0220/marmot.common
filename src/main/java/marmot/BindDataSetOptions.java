package marmot;

import java.util.Optional;

import marmot.dataset.GeometryColumnInfo;
import marmot.optor.StoreDataSetOptions;
import marmot.proto.service.BindDataSetOptionsProto;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class BindDataSetOptions {
	public static final BindDataSetOptions EMPTY
							= new BindDataSetOptions(Optional.empty(), false);
	
	private final Optional<GeometryColumnInfo> m_gcInfo;
	private final boolean m_force;
	
	private BindDataSetOptions(Optional<GeometryColumnInfo> gcInfo, boolean force) {
		m_gcInfo = gcInfo;
		m_force = force;
	}
	
	public static BindDataSetOptions DEFAULT() {
		return new BindDataSetOptions(Optional.empty(), false);
	}
	
	public static BindDataSetOptions DEFAULT(GeometryColumnInfo gcInfo) {
		return new BindDataSetOptions(Optional.of(gcInfo), false);
	}
	
	public static BindDataSetOptions FORCE(boolean flag) {
		return new BindDataSetOptions(Optional.empty(), flag);
	}
	
	public static BindDataSetOptions FORCE(GeometryColumnInfo gcInfo) {
		return new BindDataSetOptions(Optional.of(gcInfo), true);
	}
	
	public Optional<GeometryColumnInfo> geometryColumnInfo() {
		return m_gcInfo;
	}
	
	public BindDataSetOptions geometryColumnInfo(GeometryColumnInfo gcInfo) {
		return new BindDataSetOptions(Optional.of(gcInfo), m_force);
	}
	
	public boolean force() {
		return m_force;
	}
	
	public BindDataSetOptions force(Boolean flag) {
		return new BindDataSetOptions(m_gcInfo, flag);
	}
	
	public StoreDataSetOptions toStoreDataSetOptions() {
		StoreDataSetOptions opts = StoreDataSetOptions.DEFAULT.force(m_force);
		opts = m_gcInfo.isPresent() ? opts.geometryColumnInfo(m_gcInfo.get()) : opts;
		
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
