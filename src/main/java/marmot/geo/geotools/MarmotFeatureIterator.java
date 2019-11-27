package marmot.geo.geotools;

import java.util.NoSuchElementException;

import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import marmot.Record;
import marmot.RecordSet;
import marmot.support.DefaultRecord;
import utils.Utilities;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MarmotFeatureIterator implements SimpleFeatureIterator {
	private final RecordSet m_rset;
	private final SimpleFeatureBuilder m_featBuilder;
	private final Record m_record;
	private boolean m_hasNext;
	
	public MarmotFeatureIterator(SimpleFeatureType sfType, RecordSet rset) {
		Utilities.checkNotNullArgument(sfType, "SimpleFeatureType is null");
		Utilities.checkNotNullArgument(rset, "RecordSet is null");
		
		m_featBuilder = new SimpleFeatureBuilder(sfType);
		m_record = DefaultRecord.of(rset.getRecordSchema());
		
		m_rset = rset;
		m_hasNext = m_rset.next(m_record);
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

		SimpleFeature feature = m_featBuilder.buildFeature(null, m_record.getValues().toArray());
		
		m_hasNext = m_rset.next(m_record);
		if ( !m_hasNext ) {
			m_rset.closeQuietly();
		}
		
		return feature;
	}

	@Override
	public void close() {
		m_rset.close();
	}
}