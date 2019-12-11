package marmot.analysis.module;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;

import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class NormalizeParameters {
	public static final String INPUT_DATASET = "input_dataset";
	public static final String OUTPUT_DATASET = "output_dataset";
	public static final String INPUT_FEATURE_COLUMNS = "input_features";
	public static final String OUTPUT_COLUMNS = "output_features";
	
	private final Map<String,String> m_params;
	
	public static List<String> getParameterNameAll() {
		return Arrays.asList(INPUT_DATASET, OUTPUT_DATASET, INPUT_FEATURE_COLUMNS, OUTPUT_COLUMNS);
	}
	
	public NormalizeParameters() {
		m_params = Maps.newHashMap();
	}
	
	public NormalizeParameters(Map<String,String> paramsMap) {
		this();
		
		m_params.putAll(paramsMap);
	}
	
	public static NormalizeParameters fromMap(Map<String,String> paramsMap) {
		return new NormalizeParameters(paramsMap);
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
	
	public List<String> inputFeatureColumns() {
		return Arrays.asList(m_params.get(INPUT_FEATURE_COLUMNS).split("#"));
	}
	public void inputFeatureColumns(List<String> cols) {
		m_params.put(INPUT_FEATURE_COLUMNS, FStream.from(cols).join("#"));
	}
	public void inputFeatureColumns(String... cols) {
		inputFeatureColumns(Arrays.asList(cols));
	}
	
	public List<String> outputFeatureColumns() {
		return Arrays.asList(m_params.get(OUTPUT_COLUMNS).split("#"));
	}
	public void outputFeatureColumns(List<String> cols) {
		m_params.put(OUTPUT_COLUMNS, FStream.from(cols).join("#"));
	}
	public void outputFeatureColumns(String... cols) {
		outputFeatureColumns(Arrays.asList(cols));
	}
	
	@Override
	public String toString() {
		return m_params.toString();
	}
}
