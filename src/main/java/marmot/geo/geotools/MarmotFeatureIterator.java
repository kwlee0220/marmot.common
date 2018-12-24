package marmot.geo.geotools;

import java.util.NoSuchElementException;
import java.util.Objects;

import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import marmot.Record;
import marmot.RecordSet;
import marmot.support.DefaultRecord;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MarmotFeatureIterator implements SimpleFeatureIterator {
	private final String m_idPrefix;
	private final RecordSet m_rset;
	private final SimpleFeatureBuilder m_featBuilder;
	private final Record m_record;
	private boolean m_hasNext;
	private int m_count;
	
	public MarmotFeatureIterator(SimpleFeatureType sfType, RecordSet rset) {
		Objects.requireNonNull(sfType, "SimpleFeatureType is null");
		Objects.requireNonNull(rset, "RecordSet is null");
		
		m_idPrefix = sfType.getTypeName() + ".";
		
		m_featBuilder = new SimpleFeatureBuilder(sfType);
		m_record = DefaultRecord.of(rset.getRecordSchema());
		
		m_rset = rset;
		m_hasNext = m_rset.next(m_record);
		m_count = 0;
	}

	@Override
	public boolean hasNext() {
		return m_hasNext;
	}

	@Override
	public SimpleFeature next() throws NoSuchElementException {
		if ( !m_hasNext ) {
			throw new NoSuchElementException();
		}
		
		m_featBuilder.addAll(m_record.getAll());
		m_hasNext = m_rset.next(m_record);
		if ( !m_hasNext ) {
			m_rset.closeQuietly();
		}
		
		++m_count;
		SimpleFeature feature = m_featBuilder.buildFeature(m_idPrefix + m_count);
//		System.out.println(feature);
		return feature;
	}

	@Override
	public void close() {
		m_rset.close();
	}
}