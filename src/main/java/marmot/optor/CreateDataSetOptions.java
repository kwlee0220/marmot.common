package marmot.optor;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.Lists;

import utils.UnitUtils;
import utils.stream.FStream;

import marmot.dataset.DataSetType;
import marmot.dataset.GeometryColumnInfo;
import marmot.io.MarmotFileWriteOptions;
import marmot.proto.service.CreateDataSetOptionsProto;
import marmot.proto.service.DataSetTypeProto;
import marmot.support.PBSerializable;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CreateDataSetOptions implements PBSerializable<CreateDataSetOptionsProto> {
	public static final CreateDataSetOptions DEFAULT
			= new CreateDataSetOptions(Optional.empty(), MarmotFileWriteOptions.DEFAULT);
	public static final CreateDataSetOptions FORCE
			= new CreateDataSetOptions(Optional.empty(), MarmotFileWriteOptions.FORCE);
	public static final CreateDataSetOptions APPEND_IF_EXISTS
			= new CreateDataSetOptions(Optional.empty(), MarmotFileWriteOptions.APPEND_IF_EXISTS);
	
	private final DataSetType m_dsType;
	private final Optional<GeometryColumnInfo> m_gcInfo;
	private final MarmotFileWriteOptions m_writeOpts;
	
	private CreateDataSetOptions(DataSetType dsType, Optional<GeometryColumnInfo> gcInfo,
									MarmotFileWriteOptions writeOpts) {
		m_dsType = dsType;
		m_gcInfo = gcInfo;
		m_writeOpts = writeOpts;
	}
	
	private CreateDataSetOptions(Optional<GeometryColumnInfo> gcInfo, MarmotFileWriteOptions writeOpts) {
		this(DataSetType.FILE, gcInfo, writeOpts);
	}
	
	public static CreateDataSetOptions GEOMETRY(GeometryColumnInfo gcInfo) {
		return new CreateDataSetOptions(Optional.of(gcInfo), MarmotFileWriteOptions.DEFAULT);
	}
	
	public static CreateDataSetOptions FORCE(GeometryColumnInfo gcInfo) {
		return new CreateDataSetOptions(Optional.of(gcInfo), MarmotFileWriteOptions.FORCE);
	}
	
	public static CreateDataSetOptions FORCE(boolean flag) {
		return new CreateDataSetOptions(Optional.empty(), MarmotFileWriteOptions.FORCE(flag));
	}
	
	public DataSetType type() {
		return m_dsType;
	}
	
	public CreateDataSetOptions type(DataSetType type) {
		return new CreateDataSetOptions(type, m_gcInfo, m_writeOpts);
	}
	
	public Optional<GeometryColumnInfo> geometryColumnInfo() {
		return m_gcInfo;
	}
	
	public CreateDataSetOptions geometryColumnInfo(GeometryColumnInfo gcInfo) {
		return new CreateDataSetOptions(m_dsType, Optional.of(gcInfo), m_writeOpts);
	}
	
	public boolean force() {
		return m_writeOpts.force();
	}
	
	public CreateDataSetOptions force(Boolean flag) {
		return new CreateDataSetOptions(m_dsType, m_gcInfo, m_writeOpts.force(flag));
	}
	
	public Optional<Long> blockSize() {
		return m_writeOpts.blockSize();
	}

	public CreateDataSetOptions blockSize(Optional<Long> blkSize) {
		return new CreateDataSetOptions(m_dsType, m_gcInfo, m_writeOpts.blockSize(blkSize));
	}
	public CreateDataSetOptions blockSize(long blkSize) {
		return blockSize(Optional.of(blkSize));
	}

	public CreateDataSetOptions blockSize(String blkSizeStr) {
		return new CreateDataSetOptions(m_dsType, m_gcInfo, m_writeOpts.blockSize(blkSizeStr));
	}
	
	public Optional<String> compressionCodecName() {
		return m_writeOpts.compressionCodecName();
	}
	public CreateDataSetOptions compressionCodecName(Optional<String> name) {
		return new CreateDataSetOptions(m_dsType, m_gcInfo, m_writeOpts.compressionCodecName(name));
	}
	public CreateDataSetOptions compressionCodecName(String name) {
		return compressionCodecName(Optional.ofNullable(name));
	}
	
	public Optional<Map<String,String>> metaData() {
		return m_writeOpts.metaData();
	}

	public CreateDataSetOptions metaData(Map<String,String> metaData) {
		return new CreateDataSetOptions(m_dsType, m_gcInfo, m_writeOpts.metaData(metaData));
	}
	
	public MarmotFileWriteOptions writeOptions() {
		return m_writeOpts;
	}
	
	public CreateDataSetOptions writeOptions(MarmotFileWriteOptions writeOpts) {
		return new CreateDataSetOptions(m_dsType, m_gcInfo, writeOpts);
	}

	public static CreateDataSetOptions fromProto(CreateDataSetOptionsProto proto) {
		DataSetType type = DataSetType.fromString(proto.getType().name());
		
		Optional<GeometryColumnInfo> gcInfo = Optional.empty();
		switch ( proto.getOptionalGeomColInfoCase() ) {
			case GEOM_COL_INFO:
				gcInfo = Optional.of(GeometryColumnInfo.fromProto(proto.getGeomColInfo()));
				break;
			default:
		}
		
		MarmotFileWriteOptions writeOpts = MarmotFileWriteOptions.fromProto(proto.getWriteOptions());
		
		return new CreateDataSetOptions(type, gcInfo, writeOpts);
	}
	
	@Override
	public CreateDataSetOptionsProto toProto() {
		CreateDataSetOptionsProto.Builder builder
							= CreateDataSetOptionsProto.newBuilder()
														.setType(DataSetTypeProto.valueOf(m_dsType.id()))
														.setWriteOptions(m_writeOpts.toProto());
		m_gcInfo.map(GeometryColumnInfo::toProto).ifPresent(builder::setGeomColInfo);
		
		return builder.build();
	}
	
	public String toOptionsString() {
		List<String> optStrList = Lists.newArrayList();
		
		m_gcInfo.map(info -> String.format("gcinfo=%s", info))
				.ifPresent(optStrList::add);
		if ( force() ) {
			optStrList.add("force");
		}
		compressionCodecName().map(codec -> String.format("compress=%s", codec))
							.ifPresent(optStrList::add);
		blockSize().map(UnitUtils::toByteSizeString)
					.map(str -> String.format("blksz=%s", str))
					.ifPresent(optStrList::add);
		
		return FStream.from(optStrList).join(',');
	}
	
	@Override
	public String toString() {
		return String.format("CreateDataSetOptions[%s]", toOptionsString());
	}
}
