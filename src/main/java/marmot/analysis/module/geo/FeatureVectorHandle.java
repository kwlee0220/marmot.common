package marmot.analysis.module.geo;

import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;

import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.Record;
import marmot.RecordSet;
import marmot.dataset.DataSet;
import marmot.support.DefaultRecord;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FeatureVectorHandle {
	private final List<String> m_colNames;
	
	public FeatureVectorHandle(List<String> colNames) {
		m_colNames = colNames;
	}
	
	public List<String> getColumnNames() {
		return m_colNames;
	}
	
	public int length() {
		return m_colNames.size();
	}
	
	public FeatureVector take(Record record) {
		List<Object> values = m_colNames.stream()
										.map(record::get)
										.collect(Collectors.toList());
		return new FeatureVector(values);
	}
	
	public void put(FeatureVector feature, Record record) {
		for ( int i =0; i < m_colNames.size(); ++i ) {
			record.set(m_colNames.get(i), feature.get(i));
		}
	}
	
	@Override
	public String toString() {
		return FStream.from(m_colNames)
						.join(",", "{", "}");
	}
	
	public List<FeatureVector> sampleInitialCentroids(MarmotRuntime marmot,
														String dsId, double sampleRatio,
														int count) {
		DataSet ds = marmot.getDataSet(dsId);
		
		Record record = DefaultRecord.of(ds.getRecordSchema());
		
		List<FeatureVector> centroids = Lists.newArrayList();

		Plan plan = marmot.planBuilder("sample_init_centroids")
								.load(dsId)
								.sample(sampleRatio)
								.take(count)
								.build();
		try ( RecordSet rset = marmot.executeLocally(plan) ) {
			while ( rset.next(record) ) {
				FeatureVector feature = take(record);
				
				boolean hasDuplicate = centroids.stream()
										.mapToDouble(c -> feature.distance(c))
										.anyMatch(dist -> Double.compare(dist, 0d) == 0);
				if ( !hasDuplicate ) {
					centroids.add(feature);
					if ( centroids.size() >= count ) {
						return centroids;
					}
				}
			}
			
			return centroids;
		}
	}
}
