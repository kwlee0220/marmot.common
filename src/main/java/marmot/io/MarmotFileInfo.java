package marmot.io;

import java.util.Map;

import com.google.common.collect.Maps;

import marmot.RecordSchema;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MarmotFileInfo {
	private RecordSchema m_schema;
	private Map<String,String> m_properties = Maps.newHashMap();
	
	public RecordSchema getRecordSchema() {
		return m_schema;
	}
	
	void setRecordSchema(RecordSchema schema) {
		m_schema = schema;
	}
	
	public Map<String,String> getPropertyAll() {
		return m_properties;
	}
	
	public String getProperty(String name) {
		return m_properties.get(name);
	}
}