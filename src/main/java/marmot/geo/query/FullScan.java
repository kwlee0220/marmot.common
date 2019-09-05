package marmot.geo.query;

import static marmot.StoreDataSetOptions.FORCE;

import java.util.UUID;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.PlanBuilder;
import marmot.RecordSet;
import marmot.geo.GeoClientUtils;
import utils.LoggerSettable;
import utils.Utilities;
import utils.func.FOption;
import utils.func.Funcs;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FullScan implements LoggerSettable {
	private final MarmotRuntime m_marmot;
	private final DataSet m_ds;
	@Nullable private Envelope m_range;
	private double m_sampleRatio = -1;
	private FOption<Long> m_sampleCount = FOption.empty();
	private DataSet m_rangedDataSet = null;
	private Logger m_logger;
	
	private FullScan(DataSet ds) {
		Utilities.checkNotNullArgument(ds, "DataSet");
		
		m_marmot = ds.getMarmotRuntime();
		m_ds = ds;
		
		m_logger = LoggerFactory.getLogger(FullScan.class);
	}
	
	public static FullScan on(DataSet ds) {
		return new FullScan(ds);
	}
	
	public FullScan setRange(Envelope range) {
		m_range = range;
		return this;
	}
	
	public FullScan setSampleCount(FOption<Long> count) {
		Utilities.checkNotNullArgument(count);
		
		m_sampleCount = count;
		return this;
	}
	
	public FullScan setSampleCount(long count) {
		Utilities.checkArgument(count > 0, "count > 0, but: " + count);
		
		m_sampleCount = FOption.of(count);
		return this;
	}
	
	public double getSampleRatio() {
		if ( m_sampleRatio > 0 ) {
			return m_sampleRatio;
		}
		else if ( m_sampleCount.isAbsent() ) {
			return 1d;
		}
		
		long count = m_sampleCount.get();
		if ( m_range == null ) {
			// 샘플 갯수를 이용하여 샘플링 비율을 추정한다.
			return (double)count / m_ds.getRecordCount();
		}
		else {
			// 먼저 질의 영역에 속한 레코드를 질의하고, 이를 바탕으로 샘플링 비율을 계산함
			// 이때, 구한 데이터 세트는 샘플링할 때도 사용한다.
			if ( m_rangedDataSet == null ) {
				m_rangedDataSet = calcRangedDataSet();
			}
			return (double)count / m_rangedDataSet.getRecordCount();
		}
	}
	
	public FullScan setSampleRatio(double ratio) {
		m_sampleRatio = ratio;
		return this;
	}

	@Override
	public Logger getLogger() {
		return m_logger;
	}

	@Override
	public void setLogger(Logger logger) {
		m_logger = logger != null ? logger : LoggerFactory.getLogger(FullScan.class);
	}

	public RecordSet run() {
		double ratio = getSampleRatio();
		String dsId = Funcs.getIfNotNull(m_rangedDataSet, m_rangedDataSet, m_ds).getId();
		String msg = String.format("full-scan: dataset=%s, ratio=%.2f%%",
									dsId, ratio*100);
		getLogger().info(msg);

		PlanBuilder builder = m_marmot.planBuilder(msg);
		if ( m_rangedDataSet != null ) {
			builder = builder.load(dsId);
		}
		else if ( m_range != null ) {
			Geometry key = GeoClientUtils.toPolygon(m_range);
			builder = builder.query(dsId, key);
		}
		else {
			builder = builder.load(dsId);
		}
		if ( ratio < 1 ) {
			builder = builder.sample(ratio);
			// sample은 확률적으로 수행하기 때문에, 필요한 sample count 보다 많을 수 있기
			// 때문에, 명시적으로 'take()'를 추가시킨다.
			builder = m_sampleCount.transform(builder, (b,c) -> b.take(c));
		}
		Plan plan = builder.build();
		
		RecordSet result = m_marmot.executeLocally(plan);
		if ( m_rangedDataSet != null ) {
			result = result.attachCloser(rs -> {
									getLogger().info("purge temporary ranged dataset: id={}", dsId);
									m_marmot.deleteDataSet(dsId);
								});
		}
		
		return result;
	}
	
	private DataSet calcRangedDataSet() {
		String rangedDsId = "tmp/" + UUID.randomUUID().toString();
		
		Geometry key = GeoClientUtils.toPolygon(m_range);
		GeometryColumnInfo gcInfo = m_ds.getGeometryColumnInfo();
		
		Plan plan;
		plan = m_marmot.planBuilder("scan range")
						.query(m_ds.getId(), key)
						.store(rangedDsId, FORCE(gcInfo))
						.build();
		m_marmot.execute(plan);
		
		return m_marmot.getDataSet(rangedDsId);
	}
}
