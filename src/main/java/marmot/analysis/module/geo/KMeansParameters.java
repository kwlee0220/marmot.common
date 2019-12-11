package marmot.analysis.module.geo;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import marmot.support.DataUtils;
import utils.CSV;
import utils.io.IOUtils;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class KMeansParameters {
	private static final char COLUMN_SEPARATOR = '#';
	
	private static final String INPUT_DATASET = "input_dataset";
	private static final String OUTPUT_DATASET = "output_dataset";
	private static final String FEATURE_COLUMNS = "feature_columns";
	private static final String CLUSTER_COLUMN = "cluster_column";
	private static final String INIT_CENTROIDS = "initial_centroids";
	private static final String TERM_DIST = "termination_distance";
	private static final String TERM_ITER = "termination_iteration";
	
	private final Map<String,String> m_params;
	
	public static List<String> getParameterNameAll() {
		return Arrays.asList(INPUT_DATASET, OUTPUT_DATASET, FEATURE_COLUMNS, CLUSTER_COLUMN,
							INIT_CENTROIDS, TERM_DIST, TERM_ITER);
	}
	
	public KMeansParameters() {
		m_params = Maps.newHashMap();
	}
	
	public KMeansParameters(Map<String,String> paramsMap) {
		this();
		
		m_params.putAll(paramsMap);
	}
	
	public static KMeansParameters fromMap(Map<String,String> paramsMap) {
		return new KMeansParameters(paramsMap);
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
	
	
	public List<String> featureColumns() {
		return CSV.parseCsv(m_params.get(FEATURE_COLUMNS), COLUMN_SEPARATOR).toList();
	}
	public void featureColumns(List<String> cols) {
		m_params.put(FEATURE_COLUMNS, FStream.from(cols).join(COLUMN_SEPARATOR));
	}
	public void featureColumns(String... cols) {
		featureColumns(Arrays.asList(cols));
	}
	
	public String clusterColumn() {
		return (String)m_params.get(CLUSTER_COLUMN);
	}
	public void clusterColumn(String colName) {
		m_params.put(CLUSTER_COLUMN, colName);
	}
	
	@SuppressWarnings("unchecked")
	public List<FeatureVector> initialCentroids() {
		try {
			String encoded = m_params.get(INIT_CENTROIDS);
			return (List<FeatureVector>)IOUtils.deserialize(IOUtils.destringify(encoded));
		}
		catch ( Exception e ) {
			throw new RuntimeException(e);
		}
	}
	public void initialCentroids(List<FeatureVector> centroids) {
		try {
			String str = IOUtils.stringify(IOUtils.serialize(Lists.newArrayList(centroids)));
			m_params.put(INIT_CENTROIDS, str);
		}
		catch ( IOException e ) {
			throw new RuntimeException(e);
		}
	}
	
	public double terminationDistance() {
		return DataUtils.asDouble(m_params.get(TERM_DIST));
	}
	public void terminationDistance(double func) {
		m_params.put(TERM_DIST, ""+func);
	}
	
	public int terminationIteration() {
		return DataUtils.asInt(m_params.get(TERM_ITER));
	}
	public void terminationIteration(int count) {
		m_params.put(TERM_ITER, ""+count);
	}
	
	@Override
	public String toString() {
		return m_params.toString();
	}
}
