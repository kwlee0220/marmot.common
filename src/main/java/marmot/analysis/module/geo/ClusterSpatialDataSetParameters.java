package marmot.analysis.module.geo;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import marmot.analysis.module.StoreDataSetParameters;
import utils.CSV;
import utils.UnitUtils;
import utils.func.FOption;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ClusterSpatialDataSetParameters extends StoreDataSetParameters {
	private static final String INPUT_DATASET = "input_dataset";
	private static final String QUAD_KEY_LIST = "quad_key_list";
	private static final String OUTPUT_DATASET = "output_dataset";
	private static final String SAMPLE_RATIO = "sample_ratio";
	private static final String MAX_QKEY_LEN = "max_qkey_length";
	private static final String CLUSTER_SIZE = "cluster_size";
	private static final String WORKER_COUNT = "worker_count";
	
	public static List<String> getParameterNameAll() {
		return Arrays.asList(INPUT_DATASET, QUAD_KEY_LIST, OUTPUT_DATASET, SAMPLE_RATIO,
							MAX_QKEY_LEN, CLUSTER_SIZE, BLOCK_SIZE, WORKER_COUNT);
	}
	
	public ClusterSpatialDataSetParameters() {
	}
	
	public ClusterSpatialDataSetParameters(Map<String,String> paramsMap) {
		super(paramsMap);
	}
	
	public static String processName() {
		return "cluster_dataset";
	}
	
	public static ClusterSpatialDataSetParameters fromMap(Map<String,String> paramsMap) {
		return new ClusterSpatialDataSetParameters(paramsMap);
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
	
	public FOption<List<String>> quadKeyList() {
		return FOption.ofNullable(m_params.get(QUAD_KEY_LIST))
						.map(csv -> CSV.parseCsv(csv).toList());
	}
	public void quadKeyList(FOption<List<String>> quadKeyList) {
		quadKeyList.ifAbsent(() -> m_params.remove(QUAD_KEY_LIST))
					.ifPresent(this::quadKeyList);
	}
	public void quadKeyList(List<String> quadKeyList) {
		m_params.put(QUAD_KEY_LIST, FStream.from(quadKeyList).join(','));
	}
	
	public String outputDataset() {
		return (String)m_params.get(OUTPUT_DATASET);
	}
	public void outputDataset(String dsId) {
		m_params.put(OUTPUT_DATASET, dsId);
	}
	
	public double sampleRatio() {
		return FOption.ofNullable(m_params.get(SAMPLE_RATIO))
					.map(Double::parseDouble)
					.getOrThrow(() -> new IllegalArgumentException("unknown parameter: " + SAMPLE_RATIO));
	}
	public void sampleRatio(double ratio) {
		m_params.put(SAMPLE_RATIO, ""+ratio);
	}
	public void sampleRatio(String raitoStr) {
		sampleRatio(Double.parseDouble(raitoStr));
	}
	
	public FOption<Integer> maxQuadKeyLength() {
		return FOption.ofNullable(m_params.get(MAX_QKEY_LEN))
						.map(Integer::parseInt);
	}
	public void maxQuadKeyLength(int length) {
		m_params.put(MAX_QKEY_LEN, ""+length);
	}
	public void maxQuadKeyLength(FOption<Integer> length) {
		length.ifAbsent(() -> m_params.remove(MAX_QKEY_LEN))
				.ifPresent(this::maxQuadKeyLength);
	}
	
	public long clusterSize() {
		return FOption.ofNullable(m_params.get(CLUSTER_SIZE))
						.map(Long::parseLong)
						.getOrThrow(() -> new IllegalArgumentException("unknown parameter: " + CLUSTER_SIZE));
	}
	public void clusterSize(long nbytes) {
		m_params.put(CLUSTER_SIZE, ""+nbytes);
	}
	public void clusterSize(String nbytesStr) {
		long nbytes = UnitUtils.parseByteSize(nbytesStr);
		clusterSize(nbytes);
	}
	
	public FOption<Integer> workerCount() {
		return FOption.ofNullable(m_params.get(WORKER_COUNT))
					.map(Integer::parseInt);
	}
	public void workerCount(int count) {
		m_params.put(WORKER_COUNT, ""+count);
	}
	public void workerCount(String nbytesStr) {
		int count = (int)UnitUtils.parseByteSize(nbytesStr);
		m_params.put(WORKER_COUNT, ""+count);
	}
	public void workerCount(FOption<Integer> count) {
		count.ifAbsent(() -> m_params.remove(WORKER_COUNT))
			.ifPresent(this::workerCount);
	}
}
