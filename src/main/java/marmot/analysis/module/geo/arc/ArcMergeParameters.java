package marmot.analysis.module.geo.arc;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;

import marmot.support.DataUtils;
import utils.Utilities;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ArcMergeParameters {
	private static final String INPUT_DATASETS = "input_datasets";
	private static final String OUTPUT_DATASET = "output_dataset";
	private static final String FORCE = "force";
	private static final String COMPRESSION = "compression";
	private static final String BLOCK_SIZE = "block_size";
	
	private final Map<String,String> m_params;
	
	public static List<String> getParameterNameAll() {
		return Arrays.asList(INPUT_DATASETS, OUTPUT_DATASET, FORCE, COMPRESSION, BLOCK_SIZE);
	}
	
	public ArcMergeParameters() {
		m_params = Maps.newHashMap();
	}
	
	public ArcMergeParameters(Map<String,String> paramsMap) {
		this();
		
		m_params.putAll(paramsMap);
	}
	
	public static ArcMergeParameters fromMap(Map<String,String> paramsMap) {
		return new ArcMergeParameters(paramsMap);
	}
	
	public Map<String,String> toMap() {
		return m_params;
	}
	
	public String getInputDatasets() {
		String str = m_params.get(INPUT_DATASETS);
		if ( str == null ) {
			throw new IllegalArgumentException("input dataset id is missing");
		}
		
		return str;
	}
	
	/**
	 * 입력 데이터세트 식별자를 설정한다.
	 * 
	 * @param dsIdList	데이터세트 식별자.
	 */
	public void setInputDatasets(String dsIdList) {
		Utilities.checkNotNullArgument(dsIdList, "input dataset id");
		
		m_params.put(INPUT_DATASETS, dsIdList);
	}
	
	public String getOutputDataset() {
		String str = m_params.get(OUTPUT_DATASET);
		if ( str == null ) {
			throw new IllegalArgumentException("output dataset id is missing");
		}
		
		return str;
	}
	
	/**
	 * 결과 데이터세트 식별자를 설정한다.
	 * 
	 * @param dsId	데이터세트 식별자.
	 */
	public void setOutputDataset(String dsId) {
		Utilities.checkNotNullArgument(dsId, "output dataset");
		
		m_params.put(OUTPUT_DATASET, dsId);
	}
	
	public FOption<Boolean> getForce() {
		String str = m_params.get(FORCE);
		return str != null ? FOption.of(DataUtils.asBoolean(str)) : FOption.empty();
	}
	
	public void setForce(boolean flag) {
		m_params.put(FORCE, "" + flag);
	}
	
	public FOption<String> getCompressionCodecName() {
		return FOption.ofNullable(m_params.get(COMPRESSION));
	}
	
	public void setCompressionCodecName(String codecName) {
		if ( codecName != null ) {
			m_params.put(COMPRESSION, codecName);
		}
		else {
			m_params.remove(COMPRESSION);
		}
	}
	
	public FOption<Long> getBlockSize() {
		String str = m_params.get(BLOCK_SIZE);
		return str != null ? FOption.of(DataUtils.asLong(str)) : FOption.empty();
	}
	
	public void setBlockSize(long sz) {
		m_params.put(BLOCK_SIZE, "" + sz);
	}
	
	@Override
	public String toString() {
		return m_params.toString();
	}
}
