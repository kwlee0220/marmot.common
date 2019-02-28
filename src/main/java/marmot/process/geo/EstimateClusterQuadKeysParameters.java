package marmot.process.geo;

import java.util.Map;

import com.google.common.collect.Maps;

import utils.UnitUtils;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class EstimateClusterQuadKeysParameters {
	private static final String INPUT_DATASET = "input_dataset";
	private static final String OUTPUT_DATASET = "output_dataset";
	private static final String SAMPLE_RATIO = "sample_ratio";
	private static final String BLOCK_FILL_RATIO = "block_fill_ratio";
	private static final String BLOCK_SIZE = "block_size";
	
	private final Map<String,String> m_params;
	
	public EstimateClusterQuadKeysParameters() {
		m_params = Maps.newHashMap();
	}
	
	public static String processName() {
		return "estimate_cluster_quadkeys";
	}
	
	public EstimateClusterQuadKeysParameters(Map<String,String> paramsMap) {
		this();
		
		m_params.putAll(paramsMap);
	}
	
	public static EstimateClusterQuadKeysParameters fromMap(Map<String,String> paramsMap) {
		return new EstimateClusterQuadKeysParameters(paramsMap);
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
	
	public String outputDataset() {
		return (String)m_params.get(OUTPUT_DATASET);
	}
	public void outputDataset(String dsId) {
		m_params.put(OUTPUT_DATASET, dsId);
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
	
	public FOption<Double> blockFillRatio() {
		return FOption.ofNullable(m_params.get(BLOCK_FILL_RATIO))
					.map(Double::parseDouble);
	}
	public void blockFillRatio(double ratio) {
		m_params.put(BLOCK_FILL_RATIO, ""+ratio);
	}
	public void blockFillRatio(FOption<Double> ratio) {
		ratio.ifAbsent(() -> m_params.remove(BLOCK_FILL_RATIO))
			.ifPresent(this::blockFillRatio);
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
}
