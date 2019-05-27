package marmot.io;

import java.util.Map;

import com.google.common.collect.Maps;

import utils.UnitUtils;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MarmotFileWriteOptions {
	protected FOption<Boolean> m_force = FOption.empty();
	protected FOption<Boolean> m_append = FOption.empty();
	protected FOption<Long> m_blockSize = FOption.empty();
	protected FOption<Boolean> m_compress = FOption.empty();
	protected FOption<Map<String,String>> m_metaData = FOption.empty();
			
	public static MarmotFileWriteOptions create() {
		return new MarmotFileWriteOptions();
	}
	
	public FOption<Boolean> force() {
		return m_force;
	}
	
	public MarmotFileWriteOptions force(Boolean flag) {
		m_force = FOption.ofNullable(flag);
		return this;
	}
	
	public FOption<Boolean> append() {
		return m_append;
	}
	
	public MarmotFileWriteOptions append(Boolean flag) {
		m_append = FOption.ofNullable(flag);
		return this;
	}
	
	public FOption<Long> blockSize() {
		return m_blockSize;
	}

	public MarmotFileWriteOptions blockSize(long blkSize) {
		m_blockSize = FOption.of(blkSize);
		return this;
	}

	public MarmotFileWriteOptions blockSize(String blkSizeStr) {
		m_blockSize = FOption.of(UnitUtils.parseByteSize(blkSizeStr));
		return this;
	}
	
	public FOption<Boolean> compress() {
		return m_compress;
	}
	
	public MarmotFileWriteOptions compress(Boolean flag) {
		m_compress = FOption.ofNullable(flag);
		return this;
	}
	
	public FOption<Map<String,String>> metaData() {
		return m_metaData;
	}

	public MarmotFileWriteOptions metaData(Map<String,String> metaData) {
		m_metaData = FOption.of(metaData);
		return this;
	}
	
	public MarmotFileWriteOptions duplicate() {
		MarmotFileWriteOptions opts = MarmotFileWriteOptions.create();
		opts.m_force = m_force;
		opts.m_append = m_append;
		opts.m_blockSize = m_blockSize;
		opts.m_compress = m_compress;
		m_metaData.map(Maps::newHashMap).ifPresent(opts::metaData);
		
		return opts;
	}
}
