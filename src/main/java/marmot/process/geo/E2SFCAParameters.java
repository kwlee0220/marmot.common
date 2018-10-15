package marmot.process.geo;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;

import marmot.proto.process.E2SFCAParametersProto;
import marmot.support.DataUtils;
import marmot.support.PBSerializable;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class E2SFCAParameters implements PBSerializable<E2SFCAParametersProto> {
	private static final String COLUMN_SEPARATOR = "#";
	
	private static final String INPUT_DATASET = "input_dataset";
	private static final String PARAM_DATASET = "param_dataset";
	private static final String OUTPUT_DATASET = "output_dataset";
	private static final String INPUT_FEATURE_COLUMNS = "input_features";
	private static final String PARAM_FEATURE_COLUMNS = "param_features";
	private static final String OUTPUT_COLUMNS = "output_features";
	private static final String TAGGED_COLUMNS = "tagged_columns";
	private static final String RADIUS = "radius";
	private static final String DISTANCE_DECAY_FUNC = "distance_decay_func";

	public static E2SFCAParameters fromProto(E2SFCAParametersProto proto) {
		E2SFCAParameters params = new E2SFCAParameters();
		params.inputDataset(proto.getInputDataset());
		params.paramDataset(proto.getParamDataset());
		params.outputDataset(proto.getOutputDataset());
		params.inputFeatureColumns(proto.getInputFeatures().split(","));
		params.paramFeatureColumns(proto.getParamFeatures().split(","));
		params.outputFeatureColumns(proto.getOutputFeatures().split(","));
		params.taggedColumns(proto.getTaggedColumns().split(","));
		params.radius(proto.getRadius());
		params.distanceDecayFunction(DistanceDecayFunctions.fromString(proto.getDistanceDecayFunc()));
		
		return params;
	}

	@Override
	public E2SFCAParametersProto toProto() {
		return E2SFCAParametersProto.newBuilder()
							.setInputDataset(inputDataset())
							.setParamDataset(paramDataset())
							.setOutputDataset(outputDataset())
							.setInputFeatures(FStream.of(inputFeatureColumns()).join(","))
							.setParamFeatures(FStream.of(paramFeatureColumns()).join(","))
							.setOutputFeatures(FStream.of(outputFeatureColumns()).join(","))
							.setTaggedColumns(FStream.of(outputFeatureColumns()).join(","))
							.setRadius(radius())
							.setDistanceDecayFunc(distanceDecayFunction().toString())
							.build();
	}
	
	private final Map<String,String> m_params;
	
	public E2SFCAParameters() {
		m_params = Maps.newHashMap();
		m_params.put(TAGGED_COLUMNS, "");
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
	
	public String inputDataset() {
		return (String)m_params.get(INPUT_DATASET);
	}
	public void inputDataset(String dsId) {
		m_params.put(INPUT_DATASET, dsId);
	}
	
	public String paramDataset() {
		return (String)m_params.get(PARAM_DATASET);
	}
	public void paramDataset(String dsId) {
		m_params.put(PARAM_DATASET, dsId);
	}
	
	public String outputDataset() {
		return (String)m_params.get(OUTPUT_DATASET);
	}
	public void outputDataset(String dsId) {
		m_params.put(OUTPUT_DATASET, dsId);
	}
	
	public List<String> inputFeatureColumns() {
		return Arrays.asList(m_params.get(INPUT_FEATURE_COLUMNS).split(COLUMN_SEPARATOR));
	}
	public void inputFeatureColumns(List<String> cols) {
		m_params.put(INPUT_FEATURE_COLUMNS, FStream.of(cols).join(COLUMN_SEPARATOR));
	}
	public void inputFeatureColumns(String... cols) {
		inputFeatureColumns(Arrays.asList(cols));
	}
	
	public List<String> paramFeatureColumns() {
		return Arrays.asList(m_params.get(PARAM_FEATURE_COLUMNS).split(COLUMN_SEPARATOR));
	}
	public void paramFeatureColumns(List<String> cols) {
		m_params.put(PARAM_FEATURE_COLUMNS, FStream.of(cols).join(COLUMN_SEPARATOR));
	}
	public void paramFeatureColumns(String... cols) {
		paramFeatureColumns(Arrays.asList(cols));
	}
	
	public List<String> outputFeatureColumns() {
		return Arrays.asList(m_params.get(OUTPUT_COLUMNS).split(COLUMN_SEPARATOR));
	}
	public void outputFeatureColumns(List<String> cols) {
		m_params.put(OUTPUT_COLUMNS, FStream.of(cols).join(COLUMN_SEPARATOR));
	}
	public void outputFeatureColumns(String... cols) {
		outputFeatureColumns(Arrays.asList(cols));
	}
	
	public List<String> taggedColumns() {
		return Arrays.asList(m_params.get(TAGGED_COLUMNS).split(COLUMN_SEPARATOR));
	}
	public void taggedColumns(List<String> cols) {
		m_params.put(TAGGED_COLUMNS, FStream.of(cols).join(COLUMN_SEPARATOR));
	}
	public void taggedColumns(String... cols) {
		taggedColumns(Arrays.asList(cols));
	}
	
	public double radius() {
		return DataUtils.asDouble(m_params.get(RADIUS));
	}
	public void radius(double distance) {
		m_params.put(RADIUS, ""+distance);
	}
	
	public DistanceDecayFunction distanceDecayFunction() {
		return DistanceDecayFunctions.fromString((String)m_params.get(DISTANCE_DECAY_FUNC));
	}
	public void distanceDecayFunction(DistanceDecayFunction func) {
		m_params.put(DISTANCE_DECAY_FUNC, func.toString());
	}
	
	@Override
	public String toString() {
		return m_params.toString();
	}
}
