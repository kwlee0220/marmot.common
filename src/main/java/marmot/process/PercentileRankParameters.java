package marmot.process;

import java.util.HashMap;
import java.util.Map;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PercentileRankParameters {
	private static final String INPUT_DATASET = "input_dataset";
	private static final String OUTPUT_DATASET = "output_dataset";
	private static final String RANK_COLUMN = "rank_column";
	private static final String OUTPUT_COLUMN = "output_column";
	
	private final Map<String,String> m_params;
	
	public PercentileRankParameters() {
		m_params = new HashMap<>();
	}
	
	public PercentileRankParameters(Map<String,String> paramsMap) {
		this();
		
		m_params.putAll(paramsMap);
	}
	
	public static PercentileRankParameters fromMap(Map<String,String> paramsMap) {
		return new PercentileRankParameters(paramsMap);
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
	
	public String rankColumn() {
		return m_params.get(RANK_COLUMN);
	}
	public void rankColumn(String col) {
		m_params.put(RANK_COLUMN, col);
	}
	
	public String outputColumn() {
		return m_params.get(OUTPUT_COLUMN);
	}
	public void outputColumn(String col) {
		m_params.put(OUTPUT_COLUMN, col);
	}
	
	@Override
	public String toString() {
		return m_params.toString();
	}
}
