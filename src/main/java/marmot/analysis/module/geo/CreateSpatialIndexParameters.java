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
	private static final String SAMPLE_RATIO = "sample_ratio";
	private static final String BLOCK_SIZE = "block_size";
	private static final String WORKER_COUNT = "worker_count";
	
	private final Map<String,String> m_params;
	
	public static List<String> getParameterNameAll() {
		return Arrays.asList(INPUT_DATASET, SAMPLE_RATIO, BLOCK_SIZE, WORKER_COUNT);
	}
	
	public CreateSpatialIndexParameters() {
		m_params = Maps.newHashMap();
	}
	
	public static String processName() {
		return "cluster_dataset";
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
	
	public FOption<Double> sampleRatio() {
		return FOption.ofNullable(m_params.get(SAMPLE_RATIO))
					.map(Double::parseDouble);
	}
	public void sampleRatio(double ratio) {
		m_params.put(SAMPLE_RATIO, ""+ratio);
	}
	public void sampleRatio(FOption<Double> ratio) {
		ratio.ifAbsent(() -> m_params.remove(SAMPLE_RATIO))
			.ifPresent(this::sampleRatio);
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
