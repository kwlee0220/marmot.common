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
public class ArcBufferParameters {
	private static final String INPUT_DATASET = "input_dataset";
	private static final String OUTPUT_DATASET = "output_dataset";
	private static final String DISTANCE = "distance";
	private static final String DISTANCE_COLUMN = "distance_column";
	private static final String DISSOLVE = "dissolve";
	private static final String FORCE = "force";
	private static final String COMPRESSION = "compression";
	private static final String BLOCK_SIZE = "block_size";
	
	private final Map<String,String> m_params;
	
	public ArcBufferParameters() {
		m_params = Maps.newHashMap();
	}
	
	public ArcBufferParameters(Map<String,String> paramsMap) {
		this();
		
		m_params.putAll(paramsMap);
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
	
	public FOption<Double> getDistance() {
		String str = m_params.get(DISTANCE);
		if ( str != null ) {
			return FOption.of(DataUtils.asDouble(str));
		}
		else {
			return FOption.empty();
		}
	}
	
	/**
	 * 인자 데이터세트 식별자를 설정한다.
	 * 
	 * @param dsId	데이터세트 식별자.
	 */
	public void setDistance(double dist) {
		m_params.put(DISTANCE, "" + dist);
	}
	
	public FOption<String> getDistanceColumn() {
		String str = m_params.get(DISTANCE_COLUMN);
		return FOption.ofNullable(str);
	}
	
	/**
	 * 결과 속성 컬럼 리스트를 설정한다.
	 * <p>
	 * 하나 이상의 컬럼을 지정하는 경우는 쉼표(',')로 컬럼 이름이 구분되어야 한다.
	 * 
	 * @param cols	컬럼 이름 리스트.
	 */
	public void setDistanceColumn(String col) {
		Utilities.checkNotNullArgument(col, "distance column is null");
		
		m_params.put(DISTANCE_COLUMN, col);
	}
	
	public boolean getDissolve() {
		String str = m_params.get(DISSOLVE);
		return DataUtils.asBoolean(str);
	}
	
	public void setDissolve(boolean flag) {
		m_params.put(DISSOLVE, "" + flag);
	}
	
	public FOption<Boolean> getForce() {
		String str = m_params.get(FORCE);
		return str != null ? FOption.of(DataUtils.asBoolean(str)) : FOption.empty();
	}
	
	public void setForce(boolean flag) {
		m_params.put(FORCE, "" + flag);
	}
	
	public FOption<Boolean> getCompression() {
		String str = m_params.get(COMPRESSION);
		return str != null ? FOption.of(DataUtils.asBoolean(str)) : FOption.empty();
	}
	
	public void setCompression(boolean flag) {
		m_params.put(COMPRESSION, "" + flag);
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
	
	public static ArcBufferParameters fromMap(Map<String,String> paramsMap) {
		return new ArcBufferParameters(paramsMap);
	}
	
	public Map<String,String> toMap() {
		return m_params;
	}
}