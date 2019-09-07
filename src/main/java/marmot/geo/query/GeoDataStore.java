package marmot.geo.query;

import static java.util.concurrent.TimeUnit.MINUTES;
import static marmot.optor.AggregateFunction.COUNT;
import static marmot.optor.AggregateFunction.ENVELOPE;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.io.Files;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Polygon;

import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.MarmotRuntime;
import marmot.MarmotRuntimeException;
import marmot.Plan;
import marmot.Record;
import marmot.support.TextDataSetAdaptor;
import net.sf.cglib.proxy.MethodProxy;
import utils.CallHandler;
import utils.Utilities;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GeoDataStore {
	private static final Logger s_logger = LoggerFactory.getLogger(GeoDataStore.class);
	private static final int DEFAULT_DS_CACHE_EXPIRE_MINUTES = 60;
	private static final long DEFAULT_SAMPLE_COUNT = 50000;
	private static final boolean DEFAULT_USE_PREFETCH = false;
	private static final int DEFAULT_LOCAL_CACHE_COST = 20;
	
	private final MarmotRuntime m_marmot;
	private final TextDataSetAdaptor m_dsAdaptor;
	private final LoadingCache<String, DataSet> m_dsCache;
	private final DataSetPartitionCache m_cache;
	private long m_sampleCount = DEFAULT_SAMPLE_COUNT;
	private boolean m_usePrefetch = DEFAULT_USE_PREFETCH;
	private int m_maxLocalCacheCost = DEFAULT_LOCAL_CACHE_COST;
	
	private GeoDataStore(MarmotRuntime marmot, File cacheDir) throws IOException {
		m_marmot = marmot;
		m_dsAdaptor = new TextDataSetAdaptor(marmot);
		
		m_cache = new DataSetPartitionCache(marmot, cacheDir);
		m_dsCache = CacheBuilder.newBuilder()
								.expireAfterAccess(DEFAULT_DS_CACHE_EXPIRE_MINUTES, MINUTES)
								.build(new CacheLoader<String,DataSet>() {
									@Override
									public DataSet load(String dsId) throws Exception {
										DataSet ds = m_marmot.getDataSet(dsId);
										s_logger.info("load: dataset={}", dsId);
										return m_dsAdaptor.adapt(ds);
									}
								});
	}
	
	/**
	 * Marmot 서버에서 관리하는 공간 데이터세트를 위한 원격 저장소 객체를 생성한다.
	 * 
	 *  @param marmot	Marmot 서버 객체.
	 *  @param cacheDir	클라이언트 캐쉬 정보가 저장될 최상위 디렉토리 경로
	 *  @return	원격 공간 정보 저장소 객체
	 *  @throws	IOException	클라이언트 캐쉬 생성시 오류가 발생된 경우.
	 */
	public static GeoDataStore from(MarmotRuntime marmot, File cacheDir) throws IOException {
		Utilities.checkNotNullArgument(marmot, "MarmotRuntime is null");
		Utilities.checkNotNullArgument(cacheDir, "cacheDir is null");
		
		return new GeoDataStore(marmot, cacheDir);
	}

	/**
	 * Marmot 서버에서 관리하는 공간 데이터세트를 위한 원격 저장소 객체를 생성한다.
	 * <p>
	 * 클라이언트 캐쉬는 default 위치({@link Files#createTempDir}의 parent directory에 생성된다.
	 * 
	 *  @param marmot	Marmot 서버 객체.
	 *  @return	원격 공간 정보 저장소 객체
	 *  @throws	IOException	클라이언트 캐쉬 생성시 오류가 발생된 경우.
	 */
	public static GeoDataStore from(MarmotRuntime marmot) throws IOException {
		File parentDir = Files.createTempDir().getParentFile();
		File cacheDir =  new File(parentDir, "marmot_geoserver_cache");
		
		return from(marmot, cacheDir);
	}
	
	/**
	 * 현 클라이언트 공간 정보 저장소와 연결된 MarmotRuntime 객체를 반환한다.
	 * 
	 * @return	{@link MarmotRuntime} 객체.
	 */
	public MarmotRuntime getMarmotRuntime() {
		return m_marmot;
	}
	
	/**
	 * 저장소에서 포함된 모든 공간 데이터세트들을 반환한다.
	 * 
	 * @return 공간 데이터세트 객체 리스트.
	 */
	public List<DataSet> getGeoDataSetAll() {
		return FStream.from(m_marmot.getDataSetAll())
//						.filter(DataSet::isSpatiallyClustered)
						.filter(DataSet::hasGeometryColumn)
						.toList();
	}
	
	/**
	 * 주어진 식별자에 해당하는 공간 데이터세트를 반환한다.
	 * 
	 * @param dsId	질의 대상 데이터 세트의 식별자
	 * @return	데이터세트 객체.
	 */
	public DataSet getGeoDataSet(String dsId) {
		Utilities.checkNotNullArgument(dsId, "dataset id");
		
		return m_dsCache.getUnchecked(dsId);
	}
	
	/**
	 * 데이터 식별자에 해당하는 데이터 세트에서 주어진 영역과 겹치는 모든 레코드들을 검색할 
	 * 질의 객체를 생성한다.
	 * <p>
	 * 반환되는 질의 객체에 필요한 경우 추가의 설정을 한 뒤, {@link RangeQuery#run()}를 호출하여
	 * 영역과 겹치는 모드는 레코드를 획득하게 된다.
	 * 
	 *  @param dsId		질의 대상 데이터세트의 식별자.
	 *  @param range	질의 사각 영역
	 *  @return	영역 질의 객체.
	 */
	public RangeQuery createRangeQuery(String dsId, Envelope range) {
		try {
			DataSet ds = m_dsCache.getUnchecked(dsId);
			
			return new RangeQuery(ds, range, m_sampleCount, m_cache, m_usePrefetch,
									m_maxLocalCacheCost);
		}
		catch ( UncheckedExecutionException e ) {
			String msg = String.format("fails to locate DataSet: id=%s", dsId);
			throw new MarmotRuntimeException(msg, e.getCause());
		}
	}
	
	/**
	 * 공간 정보 저장소 수준에서의 샘플 갯수를 설정한다.
	 * <p>
	 * 이후 {@link #createRangeQuery(String, Envelope)}를 통해 생성된 모든 질의 객체는
	 * 기본적으로 이때 설정된 샘플 갯수를 갖게된다. 별도로 지정되지 않은 경우는
	 * {@link #DEFAULT_SAMPLE_COUNT}이 설정된다.
	 * 
	 * @param count	샘플 갯수
	 * @return	공간 정보 저장소 객체 (Fluent Interface 구성용)
	 */
	public GeoDataStore setSampleCount(long count) {
		Utilities.checkArgument(count > 0, "sample count must be larger than zero, but " + count);
		
		m_sampleCount = count;
		return this;
	}
	
	/**
	 * 사전 데이터세트 적재 여부를 설정한다.
	 * <p>
	 * 이후 {@link #createRangeQuery(String, Envelope)}를 통해 생성된 모든 질의 객체는
	 * 기본적으로 이때 설정된 적재 여부를 갖게된다. 별도로 지정되지 않은 경우는
	 * {@link #DEFAULT_USE_PREFETCH}이 설정된다.
	 * 
	 * @param flag	사전 적재 여부
	 * @return	공간 정보 저장소 객체 (Fluent Interface 구성용)
	 */
	public GeoDataStore setUsePrefetch(boolean flag) {
		m_usePrefetch = flag;
		return this;
	}

	/**
	 * 공간 정보 저장소 수준에서의 지역 캐쉬 활용 비용 최대 값를 설정한다.
	 * <p>
	 * 이후 {@link #createRangeQuery(String, Envelope)}를 통해 생성된 모든 질의 객체는
	 * 기본적으로 이때 설정된 비용 값을 갖게된다. 별도로 지정되지 않은 경우는
	 * {@link #DEFAULT_LOCAL_CACHE_COST}이 설정된다.
	 * 
	 * @param cost	최대 비용
	 * @return	공간 정보 저장소 객체 (Fluent Interface 구성용)
	 */
	public GeoDataStore setMaxLocalCacheCost(int cost) {
		Utilities.checkArgument(cost > 0, "MaxLocalCacheCost > 0, but " + cost);
		
		m_maxLocalCacheCost = cost;
		return this;
	}
	
	private DataSet asAdapted(DataSet ds) {
		DataSet cached = m_dsCache.getIfPresent(ds.getId());
		return (cached != null) ? cached : m_dsAdaptor.adapt(ds);
	}
	
	private static class Statistics {
		private long m_count;
		private Envelope m_bounds;
		
		Statistics() {
			this(-1, null);
		}
		
		Statistics(long count, Envelope bounds) {
			m_count = count;
			m_bounds = bounds;
		}
		
		void load(DataSet ds) {
			s_logger.info("aggregating: dataset[{}] count and mbr......", ds.getId());
			
			MarmotRuntime marmot = ds.getMarmotRuntime();
			GeometryColumnInfo gcInfo = ds.getGeometryColumnInfo();
			Plan plan = marmot.planBuilder("aggregate")
								.load(ds.getId())
								.aggregate(COUNT(), ENVELOPE(gcInfo.name()))
								.build();
			Record result = marmot.executeToRecord(plan).get();
			m_count = result.getLong(0);
			m_bounds = ((Polygon)result.get(1)).getEnvelopeInternal();
			
			s_logger.info("aggregated: {}", this);
		}
		
		@Override
		public String toString() {
			return String.format("statistics: count=%d,  mbr=%s", m_count, m_bounds);
		}
	}
	
	private static class GetRecordCount implements CallHandler<DataSet> {
		private final DataSet m_ds;
		private final Statistics m_stat;
		
		GetRecordCount(DataSet ds, Statistics stat) {
			m_ds = ds;
			m_stat = stat;
		}

		@Override
		public boolean test(Method method) {
			return "getRecordCount".equals(method.getName());
		}

		@Override
		public Object intercept(DataSet ds, Method method, Object[] args, MethodProxy proxy)
			throws Throwable {
			if ( m_stat.m_count < 0 ) {
				m_stat.load(m_ds);
			}
			
			return m_stat.m_count;
		}
	}
	
	private static class GetBounds implements CallHandler<DataSet> {
		private final DataSet m_ds;
		private final Statistics m_stat;
		
		GetBounds(DataSet ds, Statistics stat) {
			m_ds = ds;
			m_stat = stat;
		}

		@Override
		public boolean test(Method method) {
			return "getBounds".equals(method.getName());
		}

		@Override
		public Object intercept(DataSet ds, Method method, Object[] args, MethodProxy proxy)
			throws Throwable {
			if ( m_stat.m_bounds == null ) {
				m_stat.load(m_ds);
			}
			
			return m_stat.m_bounds;
		}
	}
}
