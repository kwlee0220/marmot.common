package marmot;

import java.util.Map;

import com.google.common.collect.Maps;

import marmot.proto.service.StoreDataSetOptionsProto;
import marmot.protobuf.PBUtils;
import marmot.support.PBSerializable;
import utils.UnitUtils;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class StoreDataSetOptions implements PBSerializable<StoreDataSetOptionsProto> {
	private FOption<GeometryColumnInfo> m_gcInfo = FOption.empty();
	private FOption<Boolean> m_force = FOption.empty();
	private FOption<Boolean> m_append = FOption.empty();
	private FOption<Long> m_blockSize = FOption.empty();
	private FOption<Boolean> m_compress = FOption.empty();
	private FOption<Map<String,String>> m_metaData = FOption.empty();
			
	public static StoreDataSetOptions create() {
		return new StoreDataSetOptions();
	}
	
	public FOption<GeometryColumnInfo> geometryColumnInfo() {
		return m_gcInfo;
	}
	
	public StoreDataSetOptions geometryColumnInfo(GeometryColumnInfo gcInfo) {
		m_gcInfo = FOption.ofNullable(gcInfo);
		return this;
	}
	
	public FOption<Boolean> force() {
		return m_force;
	}
	
	public StoreDataSetOptions force(Boolean flag) {
		m_force = FOption.ofNullable(flag);
		return this;
	}
	
	public FOption<Boolean> append() {
		return m_append;
	}
	
	public StoreDataSetOptions append(Boolean flag) {
		m_append = FOption.ofNullable(flag);
		return this;
	}
	
	public FOption<Long> blockSize() {
		return m_blockSize;
	}

	public StoreDataSetOptions blockSize(long blkSize) {
		m_blockSize = FOption.of(blkSize);
		return this;
	}

	public StoreDataSetOptions blockSize(String blkSizeStr) {
		m_blockSize = FOption.of(UnitUtils.parseByteSize(blkSizeStr));
		return this;
	}
	
	public FOption<Boolean> compression() {
		return m_compress;
	}
	
	public StoreDataSetOptions compression(Boolean flag) {
		m_compress = FOption.ofNullable(flag);
		return this;
	}
	
	public FOption<Map<String,String>> metaData() {
		return m_metaData;
	}

	public StoreDataSetOptions metaData(Map<String,String> metaData) {
		m_metaData = FOption.of(metaData);
		return this;
	}
	
	public StoreDataSetOptions duplicate() {
		StoreDataSetOptions opts = StoreDataSetOptions.create();
		opts.m_gcInfo = m_gcInfo;
		opts.m_force = m_force;
		opts.m_append = m_append;
		opts.m_blockSize = m_blockSize;
		opts.m_compress = m_compress;
		m_metaData.map(Maps::newHashMap).ifPresent(opts::metaData);
		
		return opts;
	}

	public static StoreDataSetOptions fromProto(StoreDataSetOptionsProto proto) {
		StoreDataSetOptions options = StoreDataSetOptions.create();
		
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
		switch ( proto.getOptionalAppendCase()) {
			case APPEND:
				options.append(proto.getForce());
				break;
			default:
		}
		switch ( proto.getOptionalBlockSizeCase() ) {
			case BLOCK_SIZE:
				options.blockSize(proto.getBlockSize());
				break;
			default:
		}
		switch ( proto.getOptionalCompressCase() ) {
			case COMPRESS:
				options.compression(proto.getCompress());
				break;
			default:
		}
		switch ( proto.getOptionalMetadataCase() ) {
			case METADATA:
				Map<String,String> metadata = PBUtils.fromProto(proto.getMetadata());
				options.metaData(metadata);
				break;
			default:
		}
		
		return options;
	}
	
	@Override
	public StoreDataSetOptionsProto toProto() {
		StoreDataSetOptionsProto.Builder builder = StoreDataSetOptionsProto.newBuilder();
		m_gcInfo.map(GeometryColumnInfo::toProto).ifPresent(builder::setGeomColInfo);
		m_force.map(builder::setForce);
		m_append.map(builder::setAppend);
		m_blockSize.map(builder::setBlockSize);
		m_compress.map(builder::setCompress);
		m_metaData.map(PBUtils::toProto).ifPresent(builder::setMetadata);
		
		return builder.build();
	}

}
