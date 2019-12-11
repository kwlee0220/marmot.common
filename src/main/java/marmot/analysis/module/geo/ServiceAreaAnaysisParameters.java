package marmot.analysis.module.geo;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;

import marmot.support.DataUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ServiceAreaAnaysisParameters {
	private static final String INPUT_DATASET = "input_dataset";
	private static final String NETWORK_DATASET = "network_dataset";
	private static final String OUTPUT_DATASET = "output_dataset";
	private static final String SERVICE_DISTANCE = "service_distance";
	private static final String INITIAL_RADIUS = "initial_radius";
	
	private final Map<String,String> m_params;
	
	public static List<String> getParameterNameAll() {
		return Arrays.asList(INPUT_DATASET, NETWORK_DATASET, OUTPUT_DATASET, SERVICE_DISTANCE,
							INITIAL_RADIUS);
	}
	
	public ServiceAreaAnaysisParameters() {
		m_params = Maps.newHashMap();
	}
	
	public ServiceAreaAnaysisParameters(Map<String,String> paramsMap) {
		this();
		
		m_params.putAll(paramsMap);
	}
	
	public static ServiceAreaAnaysisParameters fromMap(Map<String,String> paramsMap) {
		return new ServiceAreaAnaysisParameters(paramsMap);
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
	
	public String networkDataset() {
		return (String)m_params.get(NETWORK_DATASET);
	}
	public void networkDataset(String dsId) {
		m_params.put(NETWORK_DATASET, dsId);
	}
	
	public String outputDataset() {
		return (String)m_params.get(OUTPUT_DATASET);
	}
	public void outputDataset(String dsId) {
		m_params.put(OUTPUT_DATASET, dsId);
	}
	
	public double serviceDistance() {
		return DataUtils.asDouble(m_params.get(SERVICE_DISTANCE));
	}
	public void serviceDistance(double distance) {
		m_params.put(SERVICE_DISTANCE, ""+distance);
	}
	
	public double initialRadius() {
		return DataUtils.asDouble(m_params.get(INITIAL_RADIUS));
	}
	public void initialRadius(double radius) {
		m_params.put(INITIAL_RADIUS, ""+radius);
	}
	
	@Override
	public String toString() {
		return m_params.toString();
	}
}
