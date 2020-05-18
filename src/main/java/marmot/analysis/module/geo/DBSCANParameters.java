package marmot.analysis.module.geo;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import marmot.support.DataUtils;
import utils.Utilities;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DBSCANParameters {
	public static final String INPUT_DATASET = "input_dataset";
	public static final String OUTPUT_DATASET = "output_dataset";
	public static final String RADIUS = "radius";
	public static final String MIN_COUNT = "min_count";
	public static final String ID_COLUMN = "id_column";
	public static final String CLUSTER_COLUMN = "cluster_column";
	public static final String TYPE_COLUMN = "type_column";
	
	private final Map<String,String> m_params;
	
	public static List<String> getParameterNameAll() {
		return Arrays.asList(INPUT_DATASET, OUTPUT_DATASET, RADIUS, MIN_COUNT, ID_COLUMN,
							CLUSTER_COLUMN, TYPE_COLUMN);
	}
	
	public DBSCANParameters() {
		m_params = Maps.newHashMap();
	}
	
	public DBSCANParameters(Map<String,String> paramsMap) {
		this();
		
		m_params.putAll(paramsMap);
	}
	
	public static DBSCANParameters fromMap(Map<String,String> paramsMap) {
		return new DBSCANParameters(paramsMap);
	}
	
	public Map<String,String> toMap() {
		return m_params;
	}
	
	public String inputDataSet() {
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
	public void inputDataSet(String dsId) {
		Utilities.checkNotNullArgument(dsId, "input dataset id");
		
		m_params.put(INPUT_DATASET, dsId);
	}
	
	public String outputDataSet() {
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
	public void outputDataSet(String dsId) {
		Utilities.checkNotNullArgument(dsId, "output dataset");
		
		m_params.put(OUTPUT_DATASET, dsId);
	}
	
	public double radius() {
		String str = m_params.get(RADIUS);
		if ( str == null ) {
			throw new IllegalArgumentException("radius is missing");
		}

		return DataUtils.asDouble(str);
	}
	
	/**
	 * DBSCAN 클러스터링에서 사용할 검색 반경을 설정한다.
	 * 
	 *  @param distance	반경.
	 */
	public void radius(double distance) {
		Preconditions.checkArgument(distance > 0, "radius should be larger than zero");
		
		m_params.put(RADIUS, ""+distance);
	}
	
	public int minCount() {
		String str = m_params.get(MIN_COUNT);
		if ( str == null ) {
			throw new IllegalArgumentException("minimun count is missing");
		}
		
		return DataUtils.asInt(str);
	}
	
	public void minCount(int count) {
		Preconditions.checkArgument(count > 0, "minimum count be larger than zero");
		
		m_params.put(MIN_COUNT, ""+count);
	}
	
	public String idColumn() {
		String str = m_params.get(ID_COLUMN);
		if ( str == null ) {
			throw new IllegalArgumentException("id column is missing");
		}
		
		return str;
	}
	
	public void idColumn(String name) {
		Utilities.checkNotNullArgument(name, "id-column");
		
		m_params.put(ID_COLUMN, name);
	}
	
	public String clusterColumn() {
		String str = m_params.get(CLUSTER_COLUMN);
		if ( str == null ) {
			throw new IllegalArgumentException("cluster column is missing");
		}
		
		return str;
	}
	
	public void clusterColumn(String name) {
		Utilities.checkNotNullArgument(name, "cluster-column");
		
		m_params.put(CLUSTER_COLUMN, name);
	}
	
	public String typeColumn() {
		String str = m_params.get(TYPE_COLUMN);
		if ( str == null ) {
			throw new IllegalArgumentException("type column is missing");
		}
		
		return str;
	}
	
	public void typeColumn(String name) {
		Utilities.checkNotNullArgument(name, "type-column");
		
		m_params.put(TYPE_COLUMN, name);
	}
	
	@Override
	public String toString() {
		return m_params.toString();
	}
}
