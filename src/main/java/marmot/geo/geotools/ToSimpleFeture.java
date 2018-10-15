package marmot.geo.geotools;

import java.util.function.Function;

import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import marmot.Record;
import marmot.RecordSchema;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class ToSimpleFeture implements Function<Record,SimpleFeature> {
	private final String m_idPrefix;
	private int m_count;
	private final SimpleFeatureBuilder m_featBuilder;
	
	ToSimpleFeture(String typeName, String srs, RecordSchema schema) {
		m_idPrefix = typeName + ".";
		
		SimpleFeatureType type = GeoToolsUtils.toSimpleFeatureType(typeName, srs, schema);
		m_featBuilder = new SimpleFeatureBuilder(type);
		
		m_count = 0;
	}

	@Override
	public SimpleFeature apply(Record record) {
		m_featBuilder.addAll(record.getAll());
		return m_featBuilder.buildFeature(m_idPrefix + (++m_count));
	}
}