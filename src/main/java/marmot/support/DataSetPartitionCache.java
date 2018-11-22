package marmot.support;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalNotification;
import com.google.common.cache.Weigher;

import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.control.Option;
import marmot.DataSet;
import marmot.MarmotRuntime;
import marmot.Record;
import marmot.RecordSet;
import marmot.rset.PBInputStreamRecordSet;
import utils.StopWatch;
import utils.fostore.FileObjectHandler;
import utils.fostore.FileObjectStore;
import utils.io.IOUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DataSetPartitionCache {
	private static final Logger s_logger = LoggerFactory.getLogger(DataSetPartitionCache.class);
	
	private final LoadingCache<PartitionKey,Partition> m_cache;

	DataSetPartitionCache(LoadingCache<PartitionKey,Partition> cache) {
		m_cache = cache;
	}
	
	public List<Record> get(String dsId, String quadKey) {
		return m_cache.getUnchecked(new PartitionKey(dsId, quadKey)).m_records;
	}
	
	public List<Record> getIfPresent(String dsId, String quadKey) {
		Partition part = m_cache.getIfPresent(new PartitionKey(dsId, quadKey));
		return part != null ? part.m_records : null;
	}

	public static Builder builder() {
		return new Builder();
	}
	public static class Builder {
		private File m_storeDir;
		private Option<Long> m_maxCacheSize = Option.none();
		private Option<Tuple2<Long,TimeUnit>> m_timeout = Option.none();
		
		public DataSetPartitionCache build(MarmotRuntime marmot) {
			FileObjectStore<PartitionKey,InputStream> fileStore
					= new FileObjectStore<>(m_storeDir, new ParitionFileHandler(m_storeDir));
			
			CacheBuilder<Object, Object> builder = CacheBuilder.newBuilder();
			m_maxCacheSize.forEach(nbytes -> {
				builder.maximumWeight(nbytes);
				builder.weigher(new PartitionWeigher());
			});
			m_timeout.forEach(t -> builder.expireAfterAccess(t._1, t._2));
			
			ParitionLoader loader = new ParitionLoader(marmot, fileStore);
			LoadingCache<PartitionKey,Partition> cache = builder.build(loader);
			
			return new DataSetPartitionCache(cache);
		}
		
		public Builder fileStoreDir(File rootDir) {
			m_storeDir = rootDir;
			return this;
		}
		
		public Builder maxSize(long nbytes) {
			Preconditions.checkArgument(nbytes > 0, "maximum InMemoryParitionCache size");
			
			m_maxCacheSize = Option.some(nbytes);
			return this;
		}
		
		public Builder timeout(long timeout, TimeUnit unit) {
			m_timeout = Option.some(Tuple.of(timeout, unit));
			return this;
		}
	}
	
	private static class Partition {
		private final List<Record> m_records;
		private final int m_length;
		
		Partition(List<Record> records, int length) {
			m_records = records;
			m_length = length;
		}
	}
	
	private static class PartitionKey {
		private final String m_dsId;
		private final String m_quadKey;
		
		PartitionKey(String dsId, String quadKey) {
			Objects.requireNonNull(dsId, "DataSet id");
			Objects.requireNonNull(quadKey, "quad-key");
			
			m_dsId = dsId;
			m_quadKey = quadKey;
		}
		
		@Override
		public String toString() {
			return String.format("%s:%s", m_dsId, m_quadKey);
		}
		
		@Override
		public boolean equals(Object obj) {
			if ( this == obj ) {
				return true;
			}
			else if ( obj == null && !(obj instanceof PartitionKey) ) {
				return false;
			}
			
			PartitionKey other = (PartitionKey)obj;
			return m_dsId.equals(other.m_dsId)
				&& m_quadKey.equals(other.m_quadKey);
		}
		
		@Override
		public int hashCode() {
			return Objects.hash(m_dsId, m_quadKey);
		}
	}
	
	private static class PartitionWeigher implements Weigher<PartitionKey,Partition> {
		@Override
		public int weigh(PartitionKey key, Partition parition) {
			return (int)parition.m_length;
		}
	}
	
	private static class ParitionLoader extends CacheLoader<PartitionKey,Partition> {
		private final MarmotRuntime m_marmot;
		private final LoadingCache<String,DataSet> m_dsCache;
		private final FileObjectStore<PartitionKey,InputStream> m_fileStore;
		
		private ParitionLoader(MarmotRuntime marmot,
								FileObjectStore<PartitionKey,InputStream> fileStore) {
			m_marmot = marmot;
			m_fileStore = fileStore;
			
			m_dsCache = CacheBuilder.newBuilder()
									.expireAfterAccess(30, TimeUnit.MINUTES)
									.removalListener(this::onDataSetRemoved)
									.build(new CacheLoader<String,DataSet>() {
										@Override
										public DataSet load(String key) throws Exception {
											return m_marmot.getDataSet(key);
										}
									});
		}
		
		@Override
		public Partition load(PartitionKey key) throws Exception {
			Option<File> opartFile = m_fileStore.getFile(key);
			if ( opartFile.isDefined() ) {
				return fromFile(opartFile.get());
			}

			StopWatch watch = StopWatch.start();
			
			DataSet ds = m_dsCache.getUnchecked(key.m_dsId);
			InputStream cluster = ds.readRawSpatialCluster(key.m_quadKey);
			File file = m_fileStore.insert(key, cluster);
			Partition part = fromFile(file);
			
			watch.stop();
			
			if ( s_logger.isInfoEnabled() ) {	
				String elapsedStr = watch.getElapsedSecondString();
				double elapsed = watch.getElapsedInFloatingSeconds();
				String veloStr = String.format("%.0f", part.m_records.size() / elapsed);
				
				s_logger.info("load parition from marmot: ds={}, quad_key={}, elapsed={}, velo={}/s",
								key.m_dsId, key.m_quadKey, elapsedStr, veloStr);
			}
			
			return part;
		}
		
		private Partition fromFile(File file) throws IOException {
			try ( InputStream is = new BufferedInputStream(new FileInputStream(file));
				RecordSet rset = PBInputStreamRecordSet.from(is) ) {
				return new Partition(rset.toList(), (int)file.length());
			}
		}
		
		private void onDataSetRemoved(RemovalNotification<String,DataSet> noti) {
			String dsId = noti.getKey();
			
			s_logger.info("victim selected: dataset={}", dsId);
			
			try {
				List<PartitionKey> keys = m_fileStore.traverse()
													.filter(k -> k.m_dsId.equals(dsId))
													.collect(Collectors.toList());
				
				for ( PartitionKey key: keys ) {
					m_fileStore.remove(key);
				}
				
				m_dsCache.invalidateAll(keys);
			}
			catch ( IOException ignored ) { }
		}
	}
	
	private static class ParitionFileHandler
							implements FileObjectHandler<PartitionKey, InputStream> {
		private final File m_rootDir;
		
		ParitionFileHandler(File rootDir) {
			m_rootDir = rootDir;
		}

		@Override
		public InputStream readFileObject(File file) throws IOException {
			Objects.requireNonNull(file, "FileStore file");
			
			return new FileInputStream(file);
		}

		@Override
		public void writeFileObject(InputStream block, File file) throws IOException {
			Objects.requireNonNull(block, "InputStream");
			Objects.requireNonNull(file, "FileStore file");
			
			IOUtils.toFile(block, file);
		}

		@Override
		public File toFile(PartitionKey key) {
			return new File(new File(m_rootDir, key.m_dsId), key.m_quadKey);
		}

		@Override
		public PartitionKey toFileObjectKey(File file) {
			return new PartitionKey(file.getParentFile().getName(), file.getName());
		}

		@Override
		public boolean isVallidFile(File file) {
			return true;
		}
	}
}
