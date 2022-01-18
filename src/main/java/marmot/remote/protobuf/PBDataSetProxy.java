package marmot.remote.protobuf;

import java.util.Set;

import org.locationtech.jts.geom.Envelope;

import marmot.Plan;
import marmot.PlanBuilder;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.RecordSets.CountingRecordSet;
import marmot.dataset.DataSet;
import marmot.dataset.DataSetType;
import marmot.dataset.GeometryColumnInfo;
import marmot.dataset.IndexNotFoundException;
import marmot.dataset.NoGeometryColumnException;
import marmot.dataset.NotSpatiallyClusteredException;
import marmot.geo.catalog.DataSetInfo;
import marmot.geo.catalog.SpatialIndexInfo;
import marmot.geo.command.ClusterSpatiallyOptions;
import marmot.geo.command.CreateSpatialIndexOptions;
import marmot.geo.command.EstimateQuadKeysOptions;
import marmot.geo.query.RangeQueryEstimate;
import marmot.optor.StoreDataSetOptions;
import utils.Utilities;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PBDataSetProxy implements DataSet {
	private final PBDataSetServiceProxy m_service;
	private DataSetInfo m_info;
	
	PBDataSetProxy(PBDataSetServiceProxy service, DataSetInfo info) {
		m_service = service;
		m_info = info;
	}

	@Override
	public String getId() {
		return m_info.getId();
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_info.getRecordSchema();
	}

	@Override
	public DataSetType getType() {
		return m_info.getType();
	}
	
	@Override
	public String getDirName() {
		return m_info.getDirName();
	}

	@Override
	public boolean hasGeometryColumn() {
		return m_info.getGeometryColumnInfo().isPresent();
	}

	@Override
	public GeometryColumnInfo getGeometryColumnInfo() {
		return m_info.getGeometryColumnInfo()
					.getOrThrow(NoGeometryColumnException::new);
	}

	@Override
	public Envelope getBounds() {
		if ( hasGeometryColumn() ) {
			return new Envelope(m_info.getBounds());
		}
		else {
			throw new NoGeometryColumnException();
		}
	}

	@Override
	public long getRecordCount() {
		return m_info.getRecordCount();
	}

	@Override
	public String getHdfsPath() {
		return m_info.getFilePath();
	}

	@Override
	public long getBlockSize() {
		return m_info.getBlockSize();
	}

	@Override
	public FOption<SpatialIndexInfo> getSpatialIndexInfo() throws IndexNotFoundException {
		SpatialIndexInfo idxInfo = m_service.getDefaultSpatialIndexInfoOrNull(getId());
		return FOption.ofNullable(idxInfo);
	}

	@Override
	public FOption<String> getCompressionCodecName() {
		return m_info.getCompressionCodecName();
	}

	@Override
	public DataSet updateGeometryColumnInfo(FOption<GeometryColumnInfo> gcInfo) {
		return m_service.updateGeometryColumnInfo(getId(), gcInfo);
	}

	@Override
	public long length() {
		return m_service.getDataSetLength(getId());
	}

	@Override
	public RecordSet read() {
		return m_service.readDataSet(getId());
	}

	@Override
	public RangeQueryEstimate estimateRangeQuery(Envelope range) {
		return m_service.estimateRangeQuery(getId(), range);
	}
	
	@Override
	public RecordSet queryRange(Envelope range, int nsamples) {
		return m_service.queryRange(getId(), range, nsamples);
	}

	@Override
	public long append(RecordSet rset) {
		Utilities.checkNotNullArgument(rset, "RecordSet is null");
		
		long count = m_service.appendRecordSet(getId(), rset, FOption.empty());
		m_info = m_service.getDataSet(getId()).m_info;
		
		return count;
	}

	@Override
	public long append(RecordSet rset, String partId) {
		Utilities.checkNotNullArgument(rset, "RecordSet is null");
		
		long count = m_service.appendRecordSet(getId(), rset, FOption.of(partId));
		m_info = m_service.getDataSet(getId()).m_info;
		
		return count;
	}

	/**
	 * 주어진 레코드 세트에 Plan을 적용시킨 결과를 데이터세트에 추가한다.
	 * <p>
	 * 본 데이터세트의 스키마와 인자로 주어진 Plan의 수행결과로 생성되는 레코드세트의 스키마는 동일해야 한다.
	 * @param rset	Plan 수행시 사용할 입력 레코드세트.
	 * @param plan	저장시킬 레코드를 생성할 Plan 객체.
	 * 
	 * @return	본 데이터 세트 객체.
	 */
	public long append(RecordSet rset, Plan plan) {
		Utilities.checkNotNullArgument(rset, "RecordSet is null");
		Utilities.checkNotNullArgument(plan, "Plan is null");

		PlanBuilder builder = plan.toBuilder();
		switch ( builder.getLastOperatorProto().getOperatorCase() ) {
			case STORE_DATASET:
				throw new IllegalArgumentException("plan should not be store operator: plan=" + plan);
			default:
		}

		StoreDataSetOptions opts = StoreDataSetOptions.APPEND.blockSize(getBlockSize());
		opts = getCompressionCodecName().transform(opts, StoreDataSetOptions::compressionCodecName);
		Plan adjusted = builder.store(getId(), opts).build();
		
		try ( CountingRecordSet countingRSet = rset.asCountingRecordSet() ) {
			m_service.getMarmotRuntime().executeLocally(adjusted, rset);
			return countingRSet.getCount();
		}
	}

	@Override
	public SpatialIndexInfo createSpatialIndex(CreateSpatialIndexOptions opts) {
		return m_service.createSpatialIndex(getId(), opts);
	}

	@Override
	public void deleteSpatialIndex() {
		m_service.deleteSpatialCluster(getId());
	}

	@Override
	public RecordSet readSpatialCluster(String quadKey) {
		return m_service.readSpatialCluster(getId(), quadKey);
	}
	
	@Override
	public boolean isSpatiallyClustered() {
		switch ( getType() ) {
			case FILE:
				return hasSpatialIndex();
			case SPATIAL_CLUSTER:
				return true;
			default:
				return false;
		}
	}

	@Override
	public Set<String> getClusterQuadKeyAll() throws NotSpatiallyClusteredException {
		return m_service.getClusterQuadKeyAll(getId());
	}

	@Override
	public Set<String> estimateQuadKeys(EstimateQuadKeysOptions opts) {
		return m_service.estimateQuadKeys(getId(), opts);
	}

	@Override
	public void cluster(String outDsId, ClusterSpatiallyOptions opts) {
		m_service.cluster(getId(), outDsId, opts);
	}

	@Override
	public void createThumbnail(int sampleCount) throws IndexNotFoundException {
		m_service.createThumbnail(getId(), sampleCount);
	}

	@Override
	public boolean deleteThumbnail() {
		return m_service.deleteThumbnail(getId());
	}

	@Override
	public FOption<Float> getThumbnailRatio() {
		float ratio = m_service.getThumbnailRatio(getId());
		return (ratio >= 0) ? FOption.of(ratio) : FOption.empty();
	}
	
	@Override
	public String toString() {
		return getId();
	}
}
