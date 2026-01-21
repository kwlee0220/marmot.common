package marmot.analysis.module;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.Maps;

import utils.UnitUtils;

import marmot.optor.StoreDataSetOptions;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class StoreDataSetParameters {
	protected static final String FORCE = "force";
	protected static final String COMPRESS_CODEC = "compress_codec";	// optional
	protected static final String BLOCK_SIZE = "block_size";			// optional
	
	protected final Map<String,String> m_params = Maps.newHashMap();
	
	public static List<String> getParameterNameAll() {
		return Arrays.asList(FORCE, COMPRESS_CODEC, BLOCK_SIZE);
	}
	
	public StoreDataSetParameters() { }
	
	public StoreDataSetParameters(Map<String,String> paramsMap) {
		m_params.putAll(paramsMap);
	}
	
	public static StoreDataSetParameters fromMap(Map<String,String> paramsMap) {
		return new StoreDataSetParameters(paramsMap);
	}
	
	public Map<String,String> toMap() {
		return m_params;
	}
	
	public StoreDataSetOptions toStoreOptions() {
		return StoreDataSetOptions.FORCE(force())
								.compressionCodecName(compressionCodecName())
								.blockSize(blockSize());
	}
	
	public boolean force() {
		return Optional.ofNullable(m_params.get(FORCE))
						.map(Boolean::parseBoolean)
						.orElse(false);
	}
	public void force(boolean flag) {
		m_params.put(FORCE, "" + flag);
	}
	
	public Optional<String> compressionCodecName() {
		return Optional.ofNullable(m_params.get(COMPRESS_CODEC));
	}
	public void compressionCodecName(String codec) {
		m_params.put(COMPRESS_CODEC, codec);
	}
	public void compressionCodecName(Optional<String> codec) {
		codec.ifPresentOrElse(this::compressionCodecName, () -> m_params.remove(COMPRESS_CODEC));
	}
	
	public Optional<Long> blockSize() {
		return Optional.ofNullable(m_params.get(BLOCK_SIZE))
					.map(Long::parseLong);
	}
	public void blockSize(long nbytes) {
		m_params.put(BLOCK_SIZE, ""+nbytes);
	}
	public void blockSize(String nbytesStr) {
		long nbytes = UnitUtils.parseByteSize(nbytesStr);
		m_params.put(BLOCK_SIZE, ""+nbytes);
	}
	public void blockSize(Optional<Long> nbytes) {
		nbytes.ifPresentOrElse(this::blockSize, () -> m_params.remove(BLOCK_SIZE));
	}
	
	@Override
	public String toString() {
		return m_params.toString();
	}
}
