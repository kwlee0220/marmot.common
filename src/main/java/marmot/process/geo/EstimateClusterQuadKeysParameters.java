package marmot.process.geo;

import java.util.Map;

import com.google.common.collect.Maps;

import io.vavr.control.Option;
import utils.UnitUtils;

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
	
	public Option<Double> sampleRatio() {
		return Option.of(m_params.get(SAMPLE_RATIO))
					.map(Double::parseDouble);
	}
	public void sampleRatio(double ratio) {
		m_params.put(SAMPLE_RATIO, ""+ratio);
	}
	public void sampleRatio(Option<Double> ratio) {
		ratio.onEmpty(() -> m_params.remove(SAMPLE_RATIO))
			.forEach(this::sampleRatio);
	}
	
	public Option<Double> blockFillRatio() {
		return Option.of(m_params.get(BLOCK_FILL_RATIO))
					.map(Double::parseDouble);
	}
	public void blockFillRatio(double ratio) {
		m_params.put(BLOCK_FILL_RATIO, ""+ratio);
	}
	public void blockFillRatio(Option<Double> ratio) {
		ratio.onEmpty(() -> m_params.remove(BLOCK_FILL_RATIO))
			.forEach(this::blockFillRatio);
	}
	
	public Option<Long> blockSize() {
		return Option.of(m_params.get(BLOCK_SIZE))
					.map(Long::parseLong);
	}
	public void blockSize(long nbytes) {
		m_params.put(BLOCK_SIZE, ""+nbytes);
	}
	public void blockSize(String nbytesStr) {
		long nbytes = UnitUtils.parseByteSize(nbytesStr);
		m_params.put(BLOCK_SIZE, ""+nbytes);
	}
	public void blockSize(Option<Long> nbytes) {
		nbytes.onEmpty(() -> m_params.remove(BLOCK_SIZE))
			.forEach(this::blockSize);
	}
}
