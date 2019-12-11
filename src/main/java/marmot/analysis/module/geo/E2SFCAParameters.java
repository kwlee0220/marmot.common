package marmot.analysis.module.geo;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import marmot.optor.geo.advanced.WeightFunction;
import marmot.support.DataUtils;
import utils.Utilities;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class E2SFCAParameters {
	private static final String CONSUMER_DATASET = "consumer_dataset";
	private static final String PROVIDER_DATASET = "provider_dataset";
	private static final String OUTPUT_DATASET = "output_dataset";
	private static final String CONSUMER_FEATURE_COLUMNS = "consumer_features";
	private static final String PROVIDER_FEATURE_COLUMNS = "provider_features";
	private static final String OUTPUT_COLUMNS = "output_features";
	private static final String SERIVCE_DISTANCE = "service_distance";
	private static final String WEIGHT_FUNC = "weight_function";
	
	private final Map<String,String> m_params;
	
	public static List<String> getParameterNameAll() {
		return Arrays.asList(CONSUMER_DATASET, PROVIDER_DATASET, OUTPUT_DATASET,
							CONSUMER_FEATURE_COLUMNS, PROVIDER_FEATURE_COLUMNS, OUTPUT_COLUMNS,
							SERIVCE_DISTANCE, WEIGHT_FUNC);
	}
	
	public E2SFCAParameters() {
		m_params = Maps.newHashMap();
	}
	
	public E2SFCAParameters(Map<String,String> paramsMap) {
		this();
		
		m_params.putAll(paramsMap);
	}
	
	public static E2SFCAParameters fromMap(Map<String,String> paramsMap) {
		return new E2SFCAParameters(paramsMap);
	}
	
	public Map<String,String> toMap() {
		return m_params;
	}
	
	public String getConsumerDataset() {
		String str = m_params.get(CONSUMER_DATASET);
		if ( str == null ) {
			throw new IllegalArgumentException("consumer dataset id is missing");
		}
		
		return str;
	}
	
	/**
	 * 입력 데이터세트 식별자를 설정한다.
	 * 
	 * @param dsId	데이터세트 식별자.
	 */
	public void setConsumerDataset(String dsId) {
		Utilities.checkNotNullArgument(dsId, "consumer dataset id");
		
		m_params.put(CONSUMER_DATASET, dsId);
	}
	
	public String getProviderDataset() {
		String str = m_params.get(PROVIDER_DATASET);
		if ( str == null ) {
			throw new IllegalArgumentException("provider dataset id is missing");
		}
		
		return str;
	}
	
	/**
	 * 인자 데이터세트 식별자를 설정한다.
	 * 
	 * @param dsId	데이터세트 식별자.
	 */
	public void setProviderDataset(String dsId) {
		Utilities.checkNotNullArgument(dsId, "provider dataset id");
		
		m_params.put(PROVIDER_DATASET, dsId);
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
	
	public String getConsumerFeatureColumns() {
		String str = m_params.get(CONSUMER_FEATURE_COLUMNS);
		if ( str == null ) {
			throw new IllegalArgumentException("consumer features are missing");
		}
		
		return str;
	}
	
	/**
	 * 입력 데이터세트내 대상 속성 컬럼 리스트를 설정한다.
	 * <p>
	 * 하나 이상의 컬럼을 지정하는 경우는 쉼표(',')로 컬럼 이름이 구분되어야 한다.
	 * 
	 * @param cols	컬럼 이름 리스트.
	 */
	public void setConsumerFeatureColumns(String cols) {
		Utilities.checkNotNullArgument(cols, "consumer feature column list");
		
		m_params.put(CONSUMER_FEATURE_COLUMNS, cols);
	}
	
	public String getProviderFeatureColumns() {
		String str = m_params.get(PROVIDER_FEATURE_COLUMNS);
		if ( str == null ) {
			throw new IllegalArgumentException("provider features are missing");
		}
		
		return str;
	}
	
	/**
	 * 인자 데이터세트내 대상 속성 컬럼 리스트를 설정한다.
	 * <p>
	 * 하나 이상의 컬럼을 지정하는 경우는 쉼표(',')로 컬럼 이름이 구분되어야 한다.
	 * 
	 * @param cols	컬럼 이름 리스트.
	 */
	public void setProviderFeatureColumns(String cols) {
		Utilities.checkNotNullArgument(cols, "parameter feature column list");
		
		m_params.put(PROVIDER_FEATURE_COLUMNS, cols);
	}
	
	public String getOutputFeatureColumns() {
		String str = m_params.get(OUTPUT_COLUMNS);
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
	public void setOutputFeatureColumns(String cols) {
		Utilities.checkNotNullArgument(cols, "parameter feature column list");
		
		m_params.put(OUTPUT_COLUMNS, cols);
	}
	
	public double getServiceDistance() {
		String str = m_params.get(SERIVCE_DISTANCE);
		if ( str == null ) {
			throw new IllegalArgumentException("service distance is missing");
		}

		return DataUtils.asDouble(str);
	}
	
	/**
	 * E2SFCA 작업 1단계에서 사용하는 검색 반경을 설정한다.
	 * 
	 *  @param distance	반경.
	 */
	public void setServiceDistance(double distance) {
		Preconditions.checkArgument(distance > 0, "search radius for step1 should be larger than zero");
		
		m_params.put(SERIVCE_DISTANCE, ""+distance);
	}
	
	public WeightFunction getWeightFunction() {
		String str = m_params.get(WEIGHT_FUNC);
		if ( str == null ) {
			throw new IllegalArgumentException("weight function is missing");
		}

		return WeightFunction.fromString(str);
	}
	
	/**
	 * E2SFCA 작업 중 사용하는 weight function을 설정한다.
	 * 
	 * @param  wfunc	weight function
	 */
	public void setWeightFunction(WeightFunction wfunc) {
		Utilities.checkNotNullArgument(wfunc, "weight function");
		
		m_params.put(WEIGHT_FUNC, wfunc.toStringExpr());
	}
	
	@Override
	public String toString() {
		return m_params.toString();
	}
}
