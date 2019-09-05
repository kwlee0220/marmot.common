package marmot;

import java.util.Map;

import marmot.proto.service.CreateDataSetOptionsProto;
import marmot.protobuf.PBUtils;
import marmot.support.PBSerializable;
import utils.UnitUtils;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CreateDataSetOptions implements PBSerializable<CreateDataSetOptionsProto> {
	public static final CreateDataSetOptions EMPTY
			= new CreateDataSetOptions(FOption.empty(), FOption.empty(), FOption.empty(),
										FOption.empty(), FOption.empty());
	public static final CreateDataSetOptions FORCE
			= new CreateDataSetOptions(FOption.empty(), FOption.of(true), FOption.empty(),
										FOption.empty(), FOption.empty());
	
	
	private final FOption<GeometryColumnInfo> m_gcInfo;
	private final FOption<Boolean> m_force;
	private final FOption<Long> m_blockSize;
	private final FOption<String> m_compressionCodecName;
	private final FOption<Map<String,String>> m_metaData;
	
	private CreateDataSetOptions(FOption<GeometryColumnInfo> gcInfo, FOption<Boolean> force,
								FOption<Long> blockSize, FOption<String> compressionCodecName,
								FOption<Map<String,String>> metadata) {
		m_gcInfo = gcInfo;
		m_force = force;
		m_blockSize = blockSize;
		m_compressionCodecName = compressionCodecName;
		m_metaData = metadata;
	}
	
	public static CreateDataSetOptions GEOMETRY(GeometryColumnInfo gcInfo) {
		return new CreateDataSetOptions(FOption.of(gcInfo), FOption.empty(),
										FOption.empty(), FOption.empty(), FOption.empty());
	}
	
	public static CreateDataSetOptions FORCE(GeometryColumnInfo gcInfo) {
		return new CreateDataSetOptions(FOption.of(gcInfo), FOption.of(true),
										FOption.empty(), FOption.empty(), FOption.empty());
	}
	
	public FOption<GeometryColumnInfo> geometryColumnInfo() {
		return m_gcInfo;
	}
	
	public CreateDataSetOptions geometryColumnInfo(GeometryColumnInfo gcInfo) {
		return new CreateDataSetOptions(FOption.of(gcInfo), m_force, m_blockSize,
										m_compressionCodecName, m_metaData);
	}
	
	public FOption<Boolean> force() {
		return m_force;
	}
	
	public CreateDataSetOptions force(Boolean flag) {
		return new CreateDataSetOptions(m_gcInfo, FOption.of(flag), m_blockSize,
										m_compressionCodecName, m_metaData);
	}
	
	public FOption<Long> blockSize() {
		return m_blockSize;
	}

	public CreateDataSetOptions blockSize(long blkSize) {
		return new CreateDataSetOptions(m_gcInfo, m_force, FOption.of(blkSize),
										m_compressionCodecName, m_metaData);
	}

	public CreateDataSetOptions blockSize(String blkSizeStr) {
		return blockSize(Long.parseLong(blkSizeStr));
	}
	
	public FOption<String> compressionCodecName() {
		return m_compressionCodecName;
	}
	
	public CreateDataSetOptions compressionCodecName(String name) {
		return new CreateDataSetOptions(m_gcInfo, m_force, m_blockSize,
										FOption.ofNullable(name), m_metaData);
	}
	
	public FOption<Map<String,String>> metaData() {
		return m_metaData;
	}

	public CreateDataSetOptions metaData(Map<String,String> metaData) {
		return new CreateDataSetOptions(m_gcInfo, m_force, m_blockSize,
										m_compressionCodecName, FOption.of(metaData));
	}

	public static CreateDataSetOptions fromProto(CreateDataSetOptionsProto proto) {
		CreateDataSetOptions opts = CreateDataSetOptions.EMPTY;
		
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
	public CreateDataSetOptionsProto toProto() {
		CreateDataSetOptionsProto.Builder builder = CreateDataSetOptionsProto.newBuilder();
		m_gcInfo.map(GeometryColumnInfo::toProto).ifPresent(builder::setGeomColInfo);
		m_force.map(builder::setForce);
		m_blockSize.map(builder::setBlockSize);
		m_compressionCodecName.map(builder::setCompressionCodecName);
		m_metaData.map(PBUtils::toProto).ifPresent(builder::setMetadata);
		
		return builder.build();
	}
	
	@Override
	public String toString() {
		String gcInfoStr = m_gcInfo.map(info -> String.format(",gcinfo=%s", info)).getOrElse("");
		String blkStr = m_blockSize.map(UnitUtils::toByteSizeString)
									.map(str -> String.format(",blksz=%s", str))
									.getOrElse("");
		String forceStr = m_force.getOrElse(false) ? ",force" : "";
		String compressStr = m_compressionCodecName.map(str -> String.format(",compress=%s", str))
													.getOrElse("");
		return String.format("store_options[%s%s%s%s]", gcInfoStr, compressStr, forceStr,blkStr);
	}
}
