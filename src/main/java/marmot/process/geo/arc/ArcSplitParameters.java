package marmot.process.geo.arc;

import java.util.Map;

import com.google.common.collect.Maps;

import marmot.support.DataUtils;
import utils.Utilities;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ArcSplitParameters {
	private static final String INPUT_DATASET = "input_dataset";
	private static final String SPLIT_DATASET = "split_dataset";
	private static final String OUTPUT_DATASET = "output_dataset";
	private static final String SPLIT_KEY = "split_key";
	private static final String FORCE = "force";
	private static final String COMPRESSION = "compression";
	private static final String BLOCK_SIZE = "block_size";
	
	private final Map<String,String> m_params;
	
	public ArcSplitParameters() {
		m_params = Maps.newHashMap();
	}
	
	public ArcSplitParameters(Map<String,String> paramsMap) {
		this();
		
		m_params.putAll(paramsMap);
	}
	
	public static ArcSplitParameters fromMap(Map<String,String> paramsMap) {
		return new ArcSplitParameters(paramsMap);
	}
	
	public Map<String,String> toMap() {
		return m_params;
	}
	
	public String getInputDataset() {
		String str = m_params.get(INPUT_DATASET);
		if ( str == null ) {
			throw new IllegalArgumentException("input dataset id is missing");
		}
		
		return str;
	}
	
	/**
	 * 입력 데이터세트 식별자를 설정한다.
	 * 
	 * @param dsId	데이터세트 식별자.
	 */
	public void setInputDataset(String dsId) {
		Utilities.checkNotNullArgument(dsId, "input dataset id");
		
		m_params.put(INPUT_DATASET, dsId);
	}
	
	public String getSplitDataset() {
		String str = m_params.get(SPLIT_DATASET);
		if ( str == null ) {
			throw new IllegalArgumentException("split dataset id is missing");
		}
		
		return str;
	}
	
	/**
	 * 인자 데이터세트 식별자를 설정한다.
	 * 
	 * @param dsId	데이터세트 식별자.
	 */
	public void setSplitDataset(String dsId) {
		Utilities.checkNotNullArgument(dsId, "split dataset id");
		
		m_params.put(SPLIT_DATASET, dsId);
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
	
	public String getSplitKey() {
		String str = m_params.get(SPLIT_KEY);
		if ( str == null ) {
			throw new IllegalArgumentException("output columns are missing");
		}
		
		return str;
	}
	
	/**
	 * 결과 속성 컬럼 리스트를 설정한다.
	 * <p>
	 * 하나 이상의 컬럼을 지정하는 경우는 쉼표(',')로 컬럼 이름이 구분되어야 한다.
	 * 
	 * @param cols	컬럼 이름 리스트.
	 */
	public void setSplitKey(String cols) {
		Utilities.checkNotNullArgument(cols, "parameter feature column list");
		
		m_params.put(SPLIT_KEY, cols);
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
