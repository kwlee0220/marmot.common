package marmot.support;

import static marmot.optor.AggregateFunction.COUNT;
import static marmot.optor.AggregateFunction.ENVELOPE;

import java.lang.reflect.Method;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Polygon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.CallHandler;
import utils.ProxyUtils;

import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.Record;
import marmot.dataset.DataSet;
import marmot.dataset.DataSetType;
import marmot.dataset.GeometryColumnInfo;
import marmot.geo.catalog.SpatialIndexInfo;
import marmot.geo.query.GeoDataStore;

import net.sf.cglib.proxy.MethodProxy;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TextDataSetAdaptor {
	private static final Logger s_logger = LoggerFactory.getLogger(GeoDataStore.class);
	private static final Class<?>[] EXTRA_INTFCS = new Class<?>[] {DataSetAdapted.class}; 
	
	private final MarmotRuntime m_marmot;

	public TextDataSetAdaptor(MarmotRuntime marmot) {
		m_marmot = marmot;
	}
	
	public DataSet adapt(DataSet ds) {
		if ( ds.getType() == DataSetType.FILE ) {
			return ds;
		}
		else if ( ds.getType() == DataSetType.TEXT ) {
			if ( ds.hasGeometryColumn() && ds.hasSpatialIndex()) {
				SpatialIndexInfo idxInfo = ds.getSpatialIndexInfo().get();
				Statistics stat = new Statistics(m_marmot, idxInfo.getRecordCount(),
												idxInfo.getDataBounds());
				return (DataSet)ProxyUtils.replaceAction(ds, EXTRA_INTFCS,
											new GetRecordCount(stat), new GetBounds(stat));
			}
			else {
				Statistics stat = new Statistics(m_marmot);
				return (DataSet)ProxyUtils.replaceAction(ds, EXTRA_INTFCS,
											new GetRecordCount(stat), new GetBounds(stat));
			}
		}
		else if ( ds.getType() == DataSetType.GWAVE ) {
			return ds;
		}
		else {
			throw new AssertionError("unsupported DataSet type: " + ds.getType());
		}
	}
	
	public interface DataSetAdapted {
		public DataSet getSourceDataSet();
	}
	
	private static class Statistics {
		private final MarmotRuntime m_marmot;
		private long m_count;
		private Envelope m_bounds;
		
		Statistics(MarmotRuntime marmot) {
			this(marmot, -1, null);
		}
		
		Statistics(MarmotRuntime marmot, long count, Envelope bounds) {
			m_marmot = marmot;
			m_count = count;
			m_bounds = bounds;
		}
		
		void load(DataSet ds) {
			s_logger.info("aggregating: dataset[{}] count and mbr......", ds.getId());
			
			if ( ds.hasGeometryColumn() ) {
				GeometryColumnInfo gcInfo = ds.getGeometryColumnInfo();

				Plan plan = Plan.builder("aggregate")
									.load(ds.getId())
									.aggregate(COUNT(), ENVELOPE(gcInfo.name()))
									.build();
				Record result = m_marmot.executeToRecord(plan).get();
				m_count = result.getLong(0);
				m_bounds = ((Polygon)result.get(1)).getEnvelopeInternal();
			}
			else {
				Plan plan = Plan.builder("aggregate")
									.load(ds.getId())
									.aggregate(COUNT())
									.build();
				Record result = m_marmot.executeToRecord(plan).get();
				m_count = result.getLong(0);
				m_bounds = new Envelope();
			}
			
			s_logger.info("aggregated: {}", this);
		}
		
		@Override
		public String toString() {
			return String.format("statistics: count=%d,  mbr=%s", m_count, m_bounds);
		}
	}
	
	private static class GetRecordCount implements CallHandler {
		private final Statistics m_stat;
		
		GetRecordCount(Statistics stat) {
			m_stat = stat;
		}

		@Override
		public boolean test(Method method) {
			return "getRecordCount".equals(method.getName());
		}

		@Override
		public Object intercept(Object ds, Method method, Object[] args, MethodProxy proxy)
			throws Throwable {
			if ( m_stat.m_count < 0 ) {
				m_stat.load((DataSet)ds);
			}
			
			return m_stat.m_count;
		}
	}
	
	private static class GetBounds implements CallHandler {
		private final Statistics m_stat;
		
		GetBounds(Statistics stat) {
			m_stat = stat;
		}

		@Override
		public boolean test(Method method) {
			return "getBounds".equals(method.getName());
		}

		@Override
		public Object intercept(Object ds, Method method, Object[] args, MethodProxy proxy)
			throws Throwable {
			if ( m_stat.m_bounds == null ) {
				m_stat.load((DataSet)ds);
			}
			
			return m_stat.m_bounds;
		}
	}
}
