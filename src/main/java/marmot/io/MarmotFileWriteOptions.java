package marmot.io;

import java.util.Map;

import utils.UnitUtils;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MarmotFileWriteOptions {
	public static final MarmotFileWriteOptions EMPTY
			= new MarmotFileWriteOptions(FOption.empty(), FOption.empty(), FOption.empty(),
								FOption.empty(), FOption.empty());
	public static final MarmotFileWriteOptions FORCE
			= new MarmotFileWriteOptions(FOption.of(true), FOption.empty(), FOption.empty(),
										FOption.empty(), FOption.empty());
	
	private final FOption<Boolean> m_force;
	private final FOption<Boolean> m_append;
	private final FOption<Long> m_blockSize;
	private final FOption<Boolean> m_compress;
	private final FOption<Map<String,String>> m_metaData;
	
	private MarmotFileWriteOptions(FOption<Boolean> force, FOption<Boolean> append,
									FOption<Long> blockSize, FOption<Boolean> compress,
									FOption<Map<String,String>> metadata) {
		m_force = force;
		m_append = append;
		m_blockSize = blockSize;
		m_compress = compress;
		m_metaData = metadata;
	}
	
	public static MarmotFileWriteOptions META_DATA(Map<String,String> metadata) {
		return new MarmotFileWriteOptions(FOption.empty(), FOption.empty(), FOption.empty(),
											FOption.empty(), FOption.of(metadata));
	}
	
	public FOption<Boolean> force() {
		return m_force;
	}

	public MarmotFileWriteOptions force(Boolean flag) {
		return new MarmotFileWriteOptions(FOption.of(flag), m_append, m_blockSize,
											m_compress, m_metaData);
	}
	
	public FOption<Boolean> append() {
		return m_append;
	}
	
	public MarmotFileWriteOptions append(Boolean flag) {
		return new MarmotFileWriteOptions(m_force, FOption.of(flag), m_blockSize,
											m_compress, m_metaData);
	}
	
	public FOption<Long> blockSize() {
		return m_blockSize;
	}

	public MarmotFileWriteOptions blockSize(long blkSize) {
		return new MarmotFileWriteOptions(m_force, m_append, FOption.of(blkSize),
											m_compress, m_metaData);
	}

	public MarmotFileWriteOptions blockSize(String blkSizeStr) {
		return blockSize(UnitUtils.parseByteSize(blkSizeStr));
	}
	
	public FOption<Boolean> compress() {
		return m_compress;
	}
	
	public MarmotFileWriteOptions compress(Boolean flag) {
		return new MarmotFileWriteOptions(m_force, m_append, m_blockSize,
											FOption.of(flag), m_metaData);
	}
	
	public FOption<Map<String,String>> metaData() {
		return m_metaData;
	}

	public MarmotFileWriteOptions metaData(Map<String,String> metaData) {
		return new MarmotFileWriteOptions(m_force, m_append, m_blockSize,
											m_compress, FOption.of(metaData));
	}
}
