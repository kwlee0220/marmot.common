package marmot.optor;

import java.util.Map;

import marmot.dataset.GeometryColumnInfo;
import marmot.io.MarmotFileWriteOptions;
import marmot.proto.service.StoreDataSetOptionsProto;
import marmot.support.PBSerializable;
import utils.UnitUtils;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class StoreDataSetOptions implements PBSerializable<StoreDataSetOptionsProto> {
	public static final StoreDataSetOptions DEFAULT
			= new StoreDataSetOptions(CreateDataSetOptions.DEFAULT, FOption.empty());
	public static final StoreDataSetOptions FORCE
			= new StoreDataSetOptions(CreateDataSetOptions.FORCE, FOption.empty());
	public static final StoreDataSetOptions APPEND
			= new StoreDataSetOptions(CreateDataSetOptions.DEFAULT, FOption.of(true));
	
	private final CreateDataSetOptions m_createOpts;
	private final FOption<Boolean> m_append;
	
	private StoreDataSetOptions(CreateDataSetOptions createOpts, FOption<Boolean> append) {
		m_createOpts = createOpts;
		m_append = append;
	}
	
	public static StoreDataSetOptions GEOMETRY(GeometryColumnInfo gcInfo) {
		return new StoreDataSetOptions(CreateDataSetOptions.GEOMETRY(gcInfo), FOption.empty());
	}
	
	public static StoreDataSetOptions FORCE(GeometryColumnInfo gcInfo) {
		return new StoreDataSetOptions(CreateDataSetOptions.FORCE(gcInfo), FOption.empty());
	}
	
	public static StoreDataSetOptions FORCE(boolean flag) {
		return new StoreDataSetOptions(CreateDataSetOptions.FORCE(flag), FOption.empty());
	}
	
	public FOption<GeometryColumnInfo> geometryColumnInfo() {
		return m_createOpts.geometryColumnInfo();
	}
	
	public StoreDataSetOptions geometryColumnInfo(GeometryColumnInfo gcInfo) {
		return new StoreDataSetOptions(m_createOpts.geometryColumnInfo(gcInfo), m_append);
	}
	
	public boolean force() {
		return m_createOpts.force();
	}
	
	public StoreDataSetOptions force(Boolean flag) {
		return new StoreDataSetOptions(m_createOpts.force(flag), m_append);
	}
	
	public FOption<Boolean> append() {
		return m_append;
	}
	
	public StoreDataSetOptions append(Boolean flag) {
		return new StoreDataSetOptions(m_createOpts, FOption.of(flag));
	}
	
	public FOption<Long> blockSize() {
		return m_createOpts.blockSize();
	}

	public StoreDataSetOptions blockSize(FOption<Long> blkSize) {
		return new StoreDataSetOptions(m_createOpts.blockSize(blkSize), m_append);
	}
	public StoreDataSetOptions blockSize(long blkSize) {
		return new StoreDataSetOptions(m_createOpts.blockSize(blkSize), m_append);
	}

	public StoreDataSetOptions blockSize(String blkSizeStr) {
		return blockSize(UnitUtils.parseByteSize(blkSizeStr));
	}
	
	public FOption<String> compressionCodecName() {
		return m_createOpts.compressionCodecName();
	}

	public StoreDataSetOptions compressionCodecName(FOption<String> name) {
		return new StoreDataSetOptions(m_createOpts.compressionCodecName(name), m_append);
	}
	public StoreDataSetOptions compressionCodecName(String name) {
		return new StoreDataSetOptions(m_createOpts.compressionCodecName(name), m_append);
	}
	
	public FOption<Map<String,String>> metaData() {
		return m_createOpts.metaData();
	}

	public StoreDataSetOptions metaData(Map<String,String> metaData) {
		return new StoreDataSetOptions(m_createOpts.metaData(metaData), m_append);
	}
	
	public CreateDataSetOptions toCreateOptions() {
		return m_createOpts;
	}
	
	public MarmotFileWriteOptions writeOptions() {
		return m_createOpts.writeOptions();
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
		builder.setCreateOptions(m_createOpts.toProto());
		m_append.map(builder::setAppend);
		
		return builder.build();
	}
	
	public String toOptionsString() {
		String appendStr = m_append.getOrElse(false) ? ", append" : "";
		return String.format("%s%s", m_createOpts.toOptionsString(), appendStr);
	}
	
	@Override
	public String toString() {
		return String.format("StoreDataSetOptions[%s]", toOptionsString());
	}
}
