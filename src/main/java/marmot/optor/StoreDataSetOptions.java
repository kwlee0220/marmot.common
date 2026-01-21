package marmot.optor;

import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Map;
import java.util.Optional;

import utils.UnitUtils;

import marmot.dataset.GeometryColumnInfo;
import marmot.io.MarmotFileWriteOptions;
import marmot.proto.service.StoreDataSetOptionsProto;
import marmot.support.PBSerializable;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class StoreDataSetOptions implements PBSerializable<StoreDataSetOptionsProto>, Serializable {
	private static final long serialVersionUID = 1L;
	
	public static final StoreDataSetOptions DEFAULT
			= new StoreDataSetOptions(CreateDataSetOptions.DEFAULT, Optional.empty(), Optional.empty());
	public static final StoreDataSetOptions FORCE
			= new StoreDataSetOptions(CreateDataSetOptions.FORCE, Optional.empty(), Optional.empty());
	public static final StoreDataSetOptions APPEND
			= new StoreDataSetOptions(CreateDataSetOptions.APPEND_IF_EXISTS,
										Optional.of(true), Optional.empty());
	
	private final CreateDataSetOptions m_createOpts;
	private final Optional<Boolean> m_append;
	private final Optional<String> m_partitionId;
	
	private StoreDataSetOptions(CreateDataSetOptions createOpts, Optional<Boolean> append,
								Optional<String> partitionId) {
		m_createOpts = createOpts;
		m_append = append;
		m_partitionId = partitionId;
	}
	
	public static StoreDataSetOptions GEOMETRY(GeometryColumnInfo gcInfo) {
		return new StoreDataSetOptions(CreateDataSetOptions.GEOMETRY(gcInfo), Optional.empty(),
										Optional.empty());
	}
	
	public static StoreDataSetOptions FORCE(GeometryColumnInfo gcInfo) {
		return new StoreDataSetOptions(CreateDataSetOptions.FORCE(gcInfo), Optional.empty(),
										Optional.empty());
	}
	
	public static StoreDataSetOptions FORCE(boolean flag) {
		return new StoreDataSetOptions(CreateDataSetOptions.FORCE(flag), Optional.empty(),
										Optional.empty());
	}
	
	public static StoreDataSetOptions APPEND(String partId) {
		return new StoreDataSetOptions(CreateDataSetOptions.APPEND_IF_EXISTS, Optional.of(true),
										Optional.of(partId));
	}
	
	public Optional<GeometryColumnInfo> geometryColumnInfo() {
		return m_createOpts.geometryColumnInfo();
	}
	
	public StoreDataSetOptions geometryColumnInfo(GeometryColumnInfo gcInfo) {
		return new StoreDataSetOptions(m_createOpts.geometryColumnInfo(gcInfo), m_append, m_partitionId);
	}
	
	public boolean force() {
		return m_createOpts.force();
	}
	
	public StoreDataSetOptions force(Boolean flag) {
		return new StoreDataSetOptions(m_createOpts.force(flag), m_append, m_partitionId);
	}
	
	public Optional<Boolean> append() {
		return m_append;
	}
	
	public StoreDataSetOptions append(Boolean flag) {
		return new StoreDataSetOptions(m_createOpts, Optional.of(flag), Optional.empty());
	}
	
	public Optional<String> partitionId() {
		return m_partitionId;
	}
	public StoreDataSetOptions partitionId(String partId) {
		return new StoreDataSetOptions(m_createOpts, m_append, Optional.of(partId));
	}
	public StoreDataSetOptions partitionId(Optional<String> partId) {
		return new StoreDataSetOptions(m_createOpts, m_append, partId);
	}
	
	public Optional<Long> blockSize() {
		return m_createOpts.blockSize();
	}
	public StoreDataSetOptions blockSize(Optional<Long> blkSize) {
		return new StoreDataSetOptions(m_createOpts.blockSize(blkSize), m_append, m_partitionId);
	}
	public StoreDataSetOptions blockSize(long blkSize) {
		return new StoreDataSetOptions(m_createOpts.blockSize(blkSize), m_append, m_partitionId);
	}
	public StoreDataSetOptions blockSize(String blkSizeStr) {
		return blockSize(UnitUtils.parseByteSize(blkSizeStr));
	}
	
	public Optional<String> compressionCodecName() {
		return m_createOpts.compressionCodecName();
	}
	public StoreDataSetOptions compressionCodecName(Optional<String> name) {
		return new StoreDataSetOptions(m_createOpts.compressionCodecName(name), m_append, m_partitionId);
	}
	public StoreDataSetOptions compressionCodecName(String name) {
		return new StoreDataSetOptions(m_createOpts.compressionCodecName(name), m_append, m_partitionId);
	}
	
	public Optional<Map<String,String>> metaData() {
		return m_createOpts.metaData();
	}
	public StoreDataSetOptions metaData(Map<String,String> metaData) {
		return new StoreDataSetOptions(m_createOpts.metaData(metaData), m_append, m_partitionId);
	}
	
	public CreateDataSetOptions toCreateOptions() {
		return m_createOpts;
	}
	
	public MarmotFileWriteOptions writeOptions() {
		return m_createOpts.writeOptions();
	}
	
	public String toOptionsString() {
		String appendStr = m_append.orElse(false) ? ", append" : "";
		String partIdStr = m_partitionId.map(id -> "(" + id + ")").orElse("");
		return String.format("%s%s%s", m_createOpts.toOptionsString(), appendStr, partIdStr);
	}
	
	@Override
	public String toString() {
		return String.format("StoreDataSetOptions[%s]", toOptionsString());
	}
	
	private Object writeReplace() {
		return new SerializationProxy(this);
	}
	
	private void readObject(ObjectInputStream stream) throws InvalidObjectException {
		throw new InvalidObjectException("Use Serialization Proxy instead.");
	}

	private static class SerializationProxy implements Serializable {
		private static final long serialVersionUID = 1L;
		
		private final StoreDataSetOptionsProto m_proto;
		
		private SerializationProxy(StoreDataSetOptions opts) {
			m_proto = opts.toProto();
		}
		
		private Object readResolve() {
			return StoreDataSetOptions.fromProto(m_proto);
		}
	}

	public static StoreDataSetOptions fromProto(StoreDataSetOptionsProto proto) {
		CreateDataSetOptions createOpts = CreateDataSetOptions.fromProto(proto.getCreateOptions());
		StoreDataSetOptions opts = new StoreDataSetOptions(createOpts, Optional.empty(), Optional.empty());
		
		switch ( proto.getOptionalAppendCase()) {
			case APPEND:
				opts = opts.append(proto.getAppend());
				break;
			default:
		}
		
		switch ( proto.getOptionalPartitionIdCase() ) {
			case PARTITION_ID:
				opts = opts.partitionId(proto.getPartitionId());
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
		m_partitionId.map(builder::setPartitionId);
		
		return builder.build();
	}
}
