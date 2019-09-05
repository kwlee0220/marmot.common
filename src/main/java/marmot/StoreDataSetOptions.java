package marmot;

import java.util.Map;

import marmot.proto.service.StoreDataSetOptionsProto;
import marmot.support.PBSerializable;
import utils.UnitUtils;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class StoreDataSetOptions implements PBSerializable<StoreDataSetOptionsProto> {
	public static final StoreDataSetOptions EMPTY
			= new StoreDataSetOptions(CreateDataSetOptions.EMPTY, FOption.empty());
	public static final StoreDataSetOptions FORCE
			= new StoreDataSetOptions(CreateDataSetOptions.FORCE, FOption.empty());
	public static final StoreDataSetOptions APPEND
			= new StoreDataSetOptions(CreateDataSetOptions.EMPTY, FOption.of(true));
	
	private final CreateDataSetOptions m_createOptions;
	private final FOption<Boolean> m_append;
	
	private StoreDataSetOptions(CreateDataSetOptions createOpts, FOption<Boolean> append) {
		m_createOptions = createOpts;
		m_append = append;
	}
	
	public static StoreDataSetOptions GEOMETRY(GeometryColumnInfo gcInfo) {
		return new StoreDataSetOptions(CreateDataSetOptions.GEOMETRY(gcInfo), FOption.empty());
	}
	
	public static StoreDataSetOptions FORCE(GeometryColumnInfo gcInfo) {
		return new StoreDataSetOptions(CreateDataSetOptions.FORCE(gcInfo), FOption.empty());
	}
	
	public FOption<GeometryColumnInfo> geometryColumnInfo() {
		return m_createOptions.geometryColumnInfo();
	}
	
	public StoreDataSetOptions geometryColumnInfo(GeometryColumnInfo gcInfo) {
		return new StoreDataSetOptions(m_createOptions.geometryColumnInfo(gcInfo), m_append);
	}
	
	public FOption<Boolean> force() {
		return m_createOptions.force();
	}
	
	public StoreDataSetOptions force(Boolean flag) {
		return new StoreDataSetOptions(m_createOptions.force(flag), m_append);
	}
	
	public FOption<Boolean> append() {
		return m_append;
	}
	
	public StoreDataSetOptions append(Boolean flag) {
		return new StoreDataSetOptions(m_createOptions, FOption.of(flag));
	}
	
	public FOption<Long> blockSize() {
		return m_createOptions.blockSize();
	}

	public StoreDataSetOptions blockSize(long blkSize) {
		return new StoreDataSetOptions(m_createOptions.blockSize(blkSize), m_append);
	}

	public StoreDataSetOptions blockSize(String blkSizeStr) {
		return blockSize(Long.parseLong(blkSizeStr));
	}
	
	public FOption<String> compressionCodecName() {
		return m_createOptions.compressionCodecName();
	}
	
	public StoreDataSetOptions compressionCodecName(String name) {
		return new StoreDataSetOptions(m_createOptions.compressionCodecName(name), m_append);
	}
	
	public FOption<Map<String,String>> metaData() {
		return m_createOptions.metaData();
	}

	public StoreDataSetOptions metaData(Map<String,String> metaData) {
		return new StoreDataSetOptions(m_createOptions.metaData(metaData), m_append);
	}
	
	public CreateDataSetOptions toCreateOptions() {
		return m_createOptions;
	}

	public static StoreDataSetOptions fromProto(StoreDataSetOptionsProto proto) {
		CreateDataSetOptions createOpts = CreateDataSetOptions.fromProto(proto.getCreateOptions());
		StoreDataSetOptions opts = new StoreDataSetOptions(createOpts, FOption.empty());
		
		switch ( proto.getOptionalAppendCase()) {
			case APPEND:
				opts = opts.append(proto.getAppend());
				break;
			default:
		}
		
		return opts;
	}
	
	@Override
	public StoreDataSetOptionsProto toProto() {
		StoreDataSetOptionsProto.Builder builder = StoreDataSetOptionsProto.newBuilder();
		builder.setCreateOptions(m_createOptions.toProto());
		m_append.map(builder::setAppend);
		
		return builder.build();
	}
	
	@Override
	public String toString() {
		String gcInfoStr = m_createOptions.geometryColumnInfo()
										.map(info -> String.format(",gcinfo=%s", info)).getOrElse("");
		String blkStr = m_createOptions.blockSize()
									.map(UnitUtils::toByteSizeString)
									.map(str -> String.format(",blksz=%s", str))
									.getOrElse("");
		String forceStr = m_createOptions.force()
										.getOrElse(false) ? ",force" : "";
		String appendStr = m_append.getOrElse(false) ? ",append" : "";
		String compressStr = m_createOptions.compressionCodecName()
											.map(str -> String.format(",compress=%s", str))
													.getOrElse("");
		return String.format("store_options[%s%s%s%s%s]", gcInfoStr, compressStr, forceStr, appendStr,blkStr);
	}
}
