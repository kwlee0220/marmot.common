package marmot.geo.query;

import static java.util.concurrent.TimeUnit.MINUTES;
import static utils.Utilities.checkArgument;
import static utils.Utilities.checkNotNullArgument;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.locationtech.jts.geom.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.io.Files;
import com.google.common.util.concurrent.UncheckedExecutionException;

import marmot.MarmotRuntime;
import marmot.MarmotRuntimeException;
import marmot.dataset.DataSet;
import marmot.support.TextDataSetAdaptor;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GeoDataStore {
	private static final Logger s_logger = LoggerFactory.getLogger(GeoDataStore.class);
	private static final long DEFAULT_PARTITION_CACHE_EXPIRE_SECONDS = MINUTES.toSeconds(30);
	private static final int DEFAULT_DS_CACHE_EXPIRE_MINUTES = 60;
	private static final int DEFAULT_SAMPLE_COUNT = 50000;
	private static final boolean DEFAULT_USE_PREFETCH = false;
	private static final int DEFAULT_LOCAL_CACHE_COST = 15;
	
	private final MarmotRuntime m_marmot;
	private final TextDataSetAdaptor m_dsAdaptor;
	private final LoadingCache<String, DataSet> m_dsCache;
	private final PartitionCache m_cache;
	private final int m_sampleCount;
	private final boolean m_usePrefetch;
	private final int m_maxLocalCacheCost;
	
	public static Builder builder() {
		return new Builder();
	}
	
	public static class Builder {
		private MarmotRuntime m_marmot;
		private File m_cacheDir;
		private int m_dsCacheExpireMinutes = DEFAULT_DS_CACHE_EXPIRE_MINUTES;
		private long m_partCacheExpireSecs = DEFAULT_PARTITION_CACHE_EXPIRE_SECONDS;
		private int m_sampleCount = DEFAULT_SAMPLE_COUNT;
		private boolean m_usePrefetch = DEFAULT_USE_PREFETCH;
		private int m_maxLocalCacheCost = DEFAULT_LOCAL_CACHE_COST;
		
		private Builder() {
			File parentDir = Files.createTempDir().getParentFile();
			m_cacheDir = new File(parentDir, "marmot_geoserver_cache");
		}

		/**
		 * Marmot 서버에서 관리하는 공간 데이터세트를 위한 원격 저장소 객체를 생성한다.
		 * 
		 *  @return	원격 공간 정보 저장소 객체
		 *  @throws	IOException	클라이언트 캐쉬 생성시 오류가 발생된 경우.
		 */
		public GeoDataStore build() throws IOException {
			return new GeoDataStore(this);
		}
		
		public Builder setMarmotRuntime(MarmotRuntime marmot) {
			checkNotNullArgument(marmot);
			
			m_marmot = marmot;
			return this;
		}
		
		/**
		 * 공간 정보 저장소가 사용한 지역 캐쉬 저장소 디렉토리를 설정한다.
		 * 
		 * @param path	지역 캐쉬 저장소 디렉토리 경로명
		 */
		public Builder setCacheDir(File path) {
			checkArgument(path.isDirectory(), "invalid cache directory: path=" + path);
			
			m_cacheDir = path;
			return this;
		}
		
		public Builder setDataSetCacheExpireMinutes(int minutes) {
			checkArgument(minutes > 0, "invalid dataset cache expiration minutes: minutes=" + minutes);
			
			m_dsCacheExpireMinutes = minutes;
			return this;
		}
		
		public Builder setParitionCacheExpireSeconds(int seconds) {
			checkArgument(seconds > 0, "invalid partition cache expiration minutes: seconds=" + seconds);
			
			m_partCacheExpireSecs = seconds;
			return this;
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
		public Builder setSampleCount(int count) {
			checkArgument(count > 0, "sample count must be larger than zero, but " + count);
			
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
		public Builder setUsePrefetch(boolean flag) {
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
		public Builder setMaxLocalCacheCost(int cost) {
			checkArgument(cost > 0, "MaxLocalCacheCost > 0, but " + cost);
			
			m_maxLocalCacheCost = cost;
			return this;
		}
	}
	
	private GeoDataStore(Builder builder) throws IOException {
		m_marmot = builder.m_marmot;
		m_dsAdaptor = new TextDataSetAdaptor(m_marmot);
		
		m_dsCache = CacheBuilder.newBuilder()
								.expireAfterAccess(builder.m_dsCacheExpireMinutes, MINUTES)
								.build(new CacheLoader<String,DataSet>() {
									@Override
									public DataSet load(String dsId) throws Exception {
										DataSet ds = m_marmot.getDataSet(dsId);
										s_logger.info("load: dataset={}", dsId);
										return m_dsAdaptor.adapt(ds);
									}
								});
		m_cache = new PartitionCache(m_marmot, builder.m_cacheDir, builder.m_partCacheExpireSecs);
		m_sampleCount = builder.m_sampleCount;
		m_usePrefetch = builder.m_usePrefetch;
		m_maxLocalCacheCost = builder.m_maxLocalCacheCost;
	}
	
	/**
	 * 현 클라이언트 공간 정보 저장소와 연결된 MarmotRuntime 객체를 반환한다.
	 * 
	 * @return	{@link MarmotRuntime} 객체.
	 */
	public MarmotRuntime getMarmotRuntime() {
		return m_marmot;
	}
	
	public PartitionCache getPartitionCache() {
		return m_cache;
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
		checkNotNullArgument(dsId, "dataset id");
		
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
}
