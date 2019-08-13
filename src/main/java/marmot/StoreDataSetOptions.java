package marmot;

import java.util.Map;

import marmot.proto.service.StoreDataSetOptionsProto;
import marmot.protobuf.PBUtils;
import marmot.support.PBSerializable;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class StoreDataSetOptions implements PBSerializable<StoreDataSetOptionsProto> {
	public static final StoreDataSetOptions EMPTY
			= new StoreDataSetOptions(FOption.empty(), FOption.empty(), FOption.empty(),
										FOption.empty(), FOption.empty(), FOption.empty());
	public static final StoreDataSetOptions FORCE
			= new StoreDataSetOptions(FOption.empty(), FOption.of(true), FOption.empty(),
										FOption.empty(), FOption.empty(), FOption.empty());
	
	
	private final FOption<GeometryColumnInfo> m_gcInfo;
	private final FOption<Boolean> m_force;
	private final FOption<Boolean> m_append;
	private final FOption<Long> m_blockSize;
	private final FOption<String> m_compressionCodecName;
	private final FOption<Map<String,String>> m_metaData;
	
	private StoreDataSetOptions(FOption<GeometryColumnInfo> gcInfo, FOption<Boolean> force,
								FOption<Boolean> append, FOption<Long> blockSize,
								FOption<String> compressionCodecName,
								FOption<Map<String,String>> metadata) {
		m_gcInfo = gcInfo;
		m_force = force;
		m_append = append;
		m_blockSize = blockSize;
		m_compressionCodecName = compressionCodecName;
		m_metaData = metadata;
	}
	
	public static StoreDataSetOptions GEOMETRY(GeometryColumnInfo gcInfo) {
		return new StoreDataSetOptions(FOption.of(gcInfo), FOption.empty(),
										FOption.empty(), FOption.empty(), FOption.empty(),
										FOption.empty());
	}
	
	public static StoreDataSetOptions FORCE(GeometryColumnInfo gcInfo) {
		return new StoreDataSetOptions(FOption.of(gcInfo), FOption.of(true),
										FOption.empty(), FOption.empty(), FOption.empty(),
										FOption.empty());
	}
	
	public static StoreDataSetOptions APPEND(GeometryColumnInfo gcInfo) {
		return new StoreDataSetOptions(FOption.of(gcInfo), FOption.empty(),
										FOption.of(true), FOption.empty(), FOption.empty(),
										FOption.empty());
	}
	
	public FOption<GeometryColumnInfo> geometryColumnInfo() {
		return m_gcInfo;
	}
	
	public StoreDataSetOptions geometryColumnInfo(GeometryColumnInfo gcInfo) {
		return new StoreDataSetOptions(FOption.of(gcInfo), m_force, m_append,
										m_blockSize, m_compressionCodecName, m_metaData);
	}
	
	public FOption<Boolean> force() {
		return m_force;
	}
	
	public StoreDataSetOptions force(Boolean flag) {
		return new StoreDataSetOptions(m_gcInfo, FOption.of(flag), m_append,
										m_blockSize, m_compressionCodecName, m_metaData);
	}
	
	public FOption<Boolean> append() {
		return m_append;
	}
	
	public StoreDataSetOptions append(Boolean flag) {
		return new StoreDataSetOptions(m_gcInfo, m_force, FOption.of(flag),
										m_blockSize, m_compressionCodecName, m_metaData);
	}
	
	public FOption<Long> blockSize() {
		return m_blockSize;
	}

	public StoreDataSetOptions blockSize(long blkSize) {
		return new StoreDataSetOptions(m_gcInfo, m_force, m_append,
										FOption.of(blkSize), m_compressionCodecName, m_metaData);
	}

	public StoreDataSetOptions blockSize(String blkSizeStr) {
		return blockSize(Long.parseLong(blkSizeStr));
	}
	
	public FOption<String> compressionCodecName() {
		return m_compressionCodecName;
	}
	
	public StoreDataSetOptions compressionCodecName(String name) {
		return new StoreDataSetOptions(m_gcInfo, m_force, m_append, m_blockSize,
										FOption.ofNullable(name), m_metaData);
	}
	
	public FOption<Map<String,String>> metaData() {
		return m_metaData;
	}

	public StoreDataSetOptions metaData(Map<String,String> metaData) {
		return new StoreDataSetOptions(m_gcInfo, m_force, m_append, m_blockSize,
										m_compressionCodecName, FOption.of(metaData));
	}

	public static StoreDataSetOptions fromProto(StoreDataSetOptionsProto proto) {
		StoreDataSetOptions opts = StoreDataSetOptions.EMPTY;
		
		switch ( proto.getOptionalGeomColInfoCase() ) {
			case GEOM_COL_INFO:
				opts = opts.geometryColumnInfo(GeometryColumnInfo.fromProto(proto.getGeomColInfo()));
				break;
			default:
		}
		switch ( proto.getOptionalForceCase() ) {
			case FORCE:
				opts = opts.force(proto.getForce());
				break;
			default:
		}
		switch ( proto.getOptionalAppendCase()) {
			case APPEND:
				opts = opts.append(proto.getAppend());
				break;
			default:
		}
		switch ( proto.getOptionalBlockSizeCase() ) {
			case BLOCK_SIZE:
				opts = opts.blockSize(proto.getBlockSize());
				break;
			default:
		}
		switch ( proto.getOptionalCompressionCodecNameCase() ) {
			case COMPRESSION_CODEC_NAME:
				opts = opts.compressionCodecName(proto.getCompressionCodecName());
				break;
			default:
		}
		switch ( proto.getOptionalMetadataCase() ) {
			case METADATA:
				Map<String,String> metadata = PBUtils.fromProto(proto.getMetadata());
				opts = opts.metaData(metadata);
				break;
			default:
		}
		
		return opts;
	}
	
	@Override
	public StoreDataSetOptionsProto toProto() {
		StoreDataSetOptionsProto.Builder builder = StoreDataSetOptionsProto.newBuilder();
		m_gcInfo.map(GeometryColumnInfo::toProto).ifPresent(builder::setGeomColInfo);
		m_force.map(builder::setForce);
		m_append.map(builder::setAppend);
		m_blockSize.map(builder::setBlockSize);
		m_compressionCodecName.map(builder::setCompressionCodecName);
		m_metaData.map(PBUtils::toProto).ifPresent(builder::setMetadata);
		
		return builder.build();
	}

}
