package marmot.analysis.module.geo;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;

import utils.UnitUtils;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CreateSpatialIndexParameters {
	private static final String INPUT_DATASET = "input_dataset";
	private static final String QUADKEY_DATASET = "quadkey_dataset";	// 기존 quadkey list를 사용하는 경우
	private static final String SAMPLE_SIZE = "sample_size";			// quadkey list를 추정하는 경우
	private static final String BLOCK_SIZE = "block_size";
	private static final String WORKER_COUNT = "worker_count";
	
	private final Map<String,String> m_params;
	
	public static List<String> getParameterNameAll() {
		return Arrays.asList(INPUT_DATASET, QUADKEY_DATASET, SAMPLE_SIZE, BLOCK_SIZE, WORKER_COUNT);
	}
	
	public CreateSpatialIndexParameters() {
		m_params = Maps.newHashMap();
	}
	
	public static String moduleName() {
		return "create_spatial_index";
	}
	
	public CreateSpatialIndexParameters(Map<String,String> paramsMap) {
		this();
		
		m_params.putAll(paramsMap);
	}
	
	public static CreateSpatialIndexParameters fromMap(Map<String,String> paramsMap) {
		return new CreateSpatialIndexParameters(paramsMap);
	}
	
	public Map<String,String> toMap() {
		return m_params;
	}
	
	public String inputDataset() {
		return (String)m_params.get(INPUT_DATASET);
	}
	public void inputDataset(String dsId) {
		m_params.put(INPUT_DATASET, dsId);
	}
	
	public FOption<String> quadKeyDataSet() {
		return FOption.ofNullable(m_params.get(QUADKEY_DATASET));
	}
	public void quadKeyDataSet(FOption<String> dsId) {
		dsId.ifAbsent(() -> m_params.remove(QUADKEY_DATASET))
			.ifPresent(this::quadKeyDataSet);
	}
	public void quadKeyDataSet(String dsId) {
		m_params.put(QUADKEY_DATASET, dsId);
	}
	
	public FOption<Long> sampleSize() {
		return FOption.ofNullable(m_params.get(SAMPLE_SIZE))
						.map(Long::parseLong);
	}
	public void sampleSize(FOption<Long> nbytes) {
		nbytes.ifAbsent(() -> m_params.remove(SAMPLE_SIZE))
				.ifPresent(this::sampleSize);
	}
	public void sampleSize(long nbytes) {
		m_params.put(SAMPLE_SIZE, ""+nbytes);
	}
	public void sampleSize(String sizeStr) {
		sampleSize(Long.parseLong(sizeStr));
	}
	
	public FOption<Long> blockSize() {
		return FOption.ofNullable(m_params.get(BLOCK_SIZE))
					.map(Long::parseLong);
	}
	public void blockSize(long nbytes) {
		m_params.put(BLOCK_SIZE, ""+nbytes);
	}
	public void blockSize(String nbytesStr) {
		long nbytes = UnitUtils.parseByteSize(nbytesStr);
		m_params.put(BLOCK_SIZE, ""+nbytes);
	}
	public void blockSize(FOption<Long> nbytes) {
		nbytes.ifAbsent(() -> m_params.remove(BLOCK_SIZE))
			.ifPresent(this::blockSize);
	}
	
	public FOption<Integer> workerCount() {
		return FOption.ofNullable(m_params.get(WORKER_COUNT))
					.map(Integer::parseInt);
	}
	public void workerCount(int count) {
		m_params.put(WORKER_COUNT, ""+count);
	}
	public void workerCount(String nbytesStr) {
		int count = (int)UnitUtils.parseByteSize(nbytesStr);
		m_params.put(WORKER_COUNT, ""+count);
	}
	public void workerCount(FOption<Integer> count) {
		count.ifAbsent(() -> m_params.remove(WORKER_COUNT))
			.ifPresent(this::workerCount);
	}
}
