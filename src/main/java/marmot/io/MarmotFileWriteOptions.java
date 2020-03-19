package marmot.io;

import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;

import marmot.proto.MarmotFileWriteOptionsProto;
import marmot.protobuf.PBUtils;
import marmot.protobuf.PBValueProtos;
import marmot.support.PBSerializable;
import utils.UnitUtils;
import utils.func.FOption;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MarmotFileWriteOptions implements PBSerializable<MarmotFileWriteOptionsProto>, Serializable {
	public static final MarmotFileWriteOptions DEFAULT
									= new MarmotFileWriteOptions(false, false, FOption.empty(),
																FOption.empty(), FOption.empty());
	public static final MarmotFileWriteOptions FORCE
									= new MarmotFileWriteOptions(true, false, FOption.empty(),
																FOption.empty(), FOption.empty());
	public static final MarmotFileWriteOptions APPEND_IF_EXISTS
									= new MarmotFileWriteOptions(false, true, FOption.empty(),
																FOption.empty(), FOption.empty());
	
	private final boolean m_force;
	private final boolean m_appendIfExists;
	private final FOption<String> m_compressionCodecName;
	private final FOption<Long> m_blockSize;
	private final FOption<Map<String,String>> m_metaData;
	
	private MarmotFileWriteOptions(boolean force, boolean appendIfExists, FOption<String> codecName,
									FOption<Long> blockSize, FOption<Map<String,String>> metadata) {
		m_force = force;
		m_appendIfExists = appendIfExists;
		m_blockSize = blockSize;
		m_compressionCodecName = codecName;
		m_metaData = metadata;
	}
	
	public static MarmotFileWriteOptions FORCE(boolean force) {
		return new MarmotFileWriteOptions(force, false, FOption.empty(), FOption.empty(), FOption.empty());
	}
	
	public static MarmotFileWriteOptions META_DATA(Map<String,String> metadata) {
		return new MarmotFileWriteOptions(false, false, FOption.empty(), FOption.empty(),
											FOption.of(metadata));
	}
	
	public boolean force() {
		return m_force;
	}
	public MarmotFileWriteOptions force(Boolean flag) {
		return new MarmotFileWriteOptions(flag, m_appendIfExists, m_compressionCodecName,
											m_blockSize, m_metaData);
	}
	
	public boolean appendIfExists() {
		return m_appendIfExists;
	}
	public MarmotFileWriteOptions appendIfExists(Boolean flag) {
		return new MarmotFileWriteOptions(m_force, flag, m_compressionCodecName, m_blockSize, m_metaData);
	}
	
	public FOption<Long> blockSize() {
		return m_blockSize;
	}
	public MarmotFileWriteOptions blockSize(FOption<Long> blkSize) {
		return new MarmotFileWriteOptions(m_force, m_appendIfExists, m_compressionCodecName,
											blkSize, m_metaData);
	}
	public MarmotFileWriteOptions blockSize(long blkSize) {
		return blockSize(FOption.of(blkSize));
	}
	public MarmotFileWriteOptions blockSize(String blkSizeStr) {
		return blockSize(UnitUtils.parseByteSize(blkSizeStr));
	}
	
	public FOption<String> compressionCodecName() {
		return m_compressionCodecName;
	}
	public MarmotFileWriteOptions compressionCodecName(String name) {
		return new MarmotFileWriteOptions(m_force, m_appendIfExists, FOption.ofNullable(name),
											m_blockSize, m_metaData);
	}
	public MarmotFileWriteOptions compressionCodecName(FOption<String> name) {
		return new MarmotFileWriteOptions(m_force, m_appendIfExists, name, m_blockSize, m_metaData);
	}
	
	public FOption<Map<String,String>> metaData() {
		return m_metaData;
	}
	public MarmotFileWriteOptions metaData(Map<String,String> metaData) {
		return metaData(FOption.of(metaData));
	}
	public MarmotFileWriteOptions metaData(FOption<Map<String,String>> metaData) {
		return new MarmotFileWriteOptions(m_force, m_appendIfExists, m_compressionCodecName,
											m_blockSize, metaData);
	}
	
	@Override
	public String toString() {
		List<String> optStrList = Lists.newArrayList();

		if ( m_force ) { optStrList.add("force"); }
		if ( m_appendIfExists ) { optStrList.add("append"); }
		compressionCodecName().map(codec -> String.format("compress=%s", codec))
								.ifPresent(optStrList::add);
		blockSize().map(UnitUtils::toByteSizeString)
					.map(str -> String.format("blksz=%s", str))
					.ifPresent(optStrList::add);
		
		return FStream.from(optStrList).join(',');
	}

	public static MarmotFileWriteOptions fromProto(MarmotFileWriteOptionsProto proto) {
		MarmotFileWriteOptions opts = FORCE(proto.getForce());
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
				Map<String,String> metadata = PBValueProtos.fromProto(proto.getMetadata());
				opts = opts.metaData(metadata);
				break;
			default:
		}
		
		return opts;
	}
	
	public MarmotFileWriteOptionsProto toProto() {
		MarmotFileWriteOptionsProto.Builder builder = MarmotFileWriteOptionsProto.newBuilder()
																				.setForce(m_force);
		m_compressionCodecName.map(builder::setCompressionCodecName);
		m_blockSize.map(builder::setBlockSize);
		m_metaData.map(PBUtils::toProto).ifPresent(builder::setMetadata);
		
		return builder.build();
	}
	
	private Object writeReplace() {
		return new SerializationProxy(this);
	}
	
	private void readObject(ObjectInputStream stream) throws InvalidObjectException {
		throw new InvalidObjectException("Use Serialization Proxy instead.");
	}

	private static class SerializationProxy implements Serializable {
		private static final long serialVersionUID = 1L;
		
		private final MarmotFileWriteOptionsProto m_proto;
		
		private SerializationProxy(MarmotFileWriteOptions opts) {
			m_proto = opts.toProto();
		}
		
		private Object readResolve() {
			return MarmotFileWriteOptions.fromProto(m_proto);
		}
	}
}
