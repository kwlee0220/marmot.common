package marmot.geo.geotools;

import java.util.function.Supplier;

import org.geotools.feature.collection.BaseSimpleFeatureCollection;
import org.opengis.feature.simple.SimpleFeatureType;

import marmot.RecordSet;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MarmotFeatureCollection extends BaseSimpleFeatureCollection {
	private final Supplier<RecordSet> m_rsetSupplier;
	
	public MarmotFeatureCollection(SimpleFeatureType schema, Supplier<RecordSet> supplier) {
		super(schema);
		
		m_rsetSupplier = supplier;
	}
	
	@Override
	public MarmotFeatureIterator features() {
		return new MarmotFeatureIterator(getSchema(), m_rsetSupplier.get());
	}
}