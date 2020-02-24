package marmot.analysis.module.geo;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import marmot.analysis.module.StoreDataSetParameters;
import utils.UnitUtils;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ClusterSpatialDataSetParameters extends StoreDataSetParameters {
	private static final String INPUT_DATASET = "input_dataset";
	private static final String OUTPUT_DATASET = "output_dataset";
	private static final String QUADKEY_DATASET = "quadkey_dataset";
	private static final String SAMPLE_SIZE = "sample_size";		// estimation을 하는 경우
	private static final String MAX_QKEY_LEN = "max_qkey_length";	// estimation을 하는 경우
	private static final String CLUSTER_SIZE = "cluster_size";
	private static final String WORKER_COUNT = "worker_count";
	
	public static List<String> getParameterNameAll() {
		return Arrays.asList(INPUT_DATASET, OUTPUT_DATASET, QUADKEY_DATASET,
								SAMPLE_SIZE, MAX_QKEY_LEN,
								CLUSTER_SIZE, WORKER_COUNT,
								FORCE, COMPRESS_CODEC, BLOCK_SIZE);
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
	
	public String inputDataSet() {
		return (String)m_params.get(INPUT_DATASET);
	}
	public void inputDataSet(String dsId) {
		m_params.put(INPUT_DATASET, dsId);
	}
	
	public String outputDataSet() {
		return (String)m_params.get(OUTPUT_DATASET);
	}
	public void outputDataSet(String dsId) {
		m_params.put(OUTPUT_DATASET, dsId);
	}
	
	public FOption<String> quadKeyDataSet() {
		return FOption.ofNullable(m_params.get(QUADKEY_DATASET));
	}
	public void quadKeyDataSet(FOption<String> dsId) {
		dsId.ifAbsent(() -> m_params.remove(QUADKEY_DATASET))
			.ifPresent(this::quadKeyDataSet);
	}
	public void quadKeyDataSet(String dsId) {
		m_params.put(QUADKEY_DATASET, dsId);
	}
	
	public long sampleSize() {
		return FOption.ofNullable(m_params.get(SAMPLE_SIZE))
						.map(Long::parseLong)
						.getOrThrow(() -> new IllegalArgumentException("unknown parameter: " + SAMPLE_SIZE));
	}
	public void sampleSize(FOption<Long> nbytes) {
		nbytes.ifAbsent(() -> m_params.remove(SAMPLE_SIZE))
				.ifPresent(this::sampleSize);
	}
	public void sampleSize(long nbytes) {
		m_params.put(SAMPLE_SIZE, ""+nbytes);
	}
	public void sampleSize(String sizeStr) {
		sampleSize(Long.parseLong(sizeStr));
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
