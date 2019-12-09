package marmot.geo.query;

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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalNotification;

import marmot.DataSet;
import marmot.MarmotRuntime;
import marmot.RecordSet;
import marmot.protobuf.PBRecordProtos;
import utils.Utilities;
import utils.fostore.FileObjectHandler;
import utils.fostore.FileObjectStore;
import utils.func.FOption;
import utils.io.IOUtils;
import utils.io.Lz4Compressions;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DataSetPartitionCache {
	private static final Logger s_logger = LoggerFactory.getLogger(DataSetPartitionCache.class);
	private static final int DS_CACHE_EXPIRE_MINUTES = 30;

	private final LoadingCache<String,DataSet> m_dsCache;
	private final FileObjectStore<PartitionKey,InputStream> m_partitionCache;

	public DataSetPartitionCache(MarmotRuntime marmot, File storeRoot)
		throws IOException {
		s_logger.info("use dataset_partition_cache: {}", storeRoot);
		
		m_partitionCache = new FileObjectStore<>(storeRoot, new ParitionFileHandler(storeRoot));
		m_dsCache = CacheBuilder.newBuilder()
								.expireAfterAccess(DS_CACHE_EXPIRE_MINUTES, TimeUnit.MINUTES)
								.removalListener(this::onDataSetRemoved)
								.build(new CacheLoader<String,DataSet>() {
									@Override
									public DataSet load(String key) throws Exception {
										return marmot.getDataSet(key);
									}
								});
	}
	
	public boolean exists(String dsId, String quadKey) {
		return m_partitionCache.exists(new PartitionKey(dsId, quadKey));
	}
	
	public RecordSet get(String dsId, String quadKey) throws IOException {
		InputStream is;
		
		PartitionKey key = new PartitionKey(dsId, quadKey);
		FOption<InputStream> ois = m_partitionCache.get(key);
		if ( ois.isPresent() ) {	// cache에 해당 파티션이 존재하는 경
			is = ois.getUnchecked();
		}
		else {	// cache에 파티션이 존재하지 않는 경우
			DataSet ds = m_dsCache.getUnchecked(key.m_dsId);
			RecordSet cluster = ds.readSpatialCluster(key.m_quadKey);
			
			File file = writeIntoCache(key, cluster);
			is = new FileInputStream(file);
		}
		
		is = Lz4Compressions.decompress(is);
		return PBRecordProtos.readRecordSet(is);
	}
	
	public void put(String dsId, String quadKey, RecordSet rset)
		throws IOException {
		writeIntoCache(new PartitionKey(dsId, quadKey), rset);
	}
	
	public void remove(String dsId, String quadKey) {
		m_partitionCache.remove(new PartitionKey(dsId, quadKey));
	}
	
	public File getTopDir() {
		return m_partitionCache.getRootDir();
	}
	
	private File writeIntoCache(PartitionKey key, RecordSet rset)
		throws IOException {
		rset = RecordSet.from(rset.getRecordSchema(), rset.fstream().shuffle());
		
		InputStream is = PBRecordProtos.toInputStream(rset); 
		try {
			is = Lz4Compressions.compress(is);
			return m_partitionCache.insert(key, is);
		}
		finally {
			is.close();
		}
	}
	
	private void onDataSetRemoved(RemovalNotification<String,DataSet> noti) {
		String dsId = noti.getKey();
		
		s_logger.info("victim selected: dataset={}", dsId);
		
		try {
			List<PartitionKey> keys = m_partitionCache.traverse()
												.filter(k -> k.m_dsId.equals(dsId))
												.collect(Collectors.toList());
			
			for ( PartitionKey key: keys ) {
				m_partitionCache.remove(key);
			}
			
			m_dsCache.invalidateAll(keys);
		}
		catch ( IOException ignored ) { }
	}
	
	private static final class PartitionKey {
		private final String m_dsId;
		private final String m_quadKey;
		
		PartitionKey(String dsId, String quadKey) {
			Utilities.checkNotNullArgument(dsId, "DataSet id");
			Utilities.checkNotNullArgument(quadKey, "quad-key");
			
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
	
	private static final class ParitionFileHandler
								implements FileObjectHandler<PartitionKey, InputStream> {
		private final File m_rootDir;
		
		ParitionFileHandler(File rootDir) {
			m_rootDir = rootDir;
		}

		@Override
		public InputStream readFileObject(File file) throws IOException {
			Utilities.checkNotNullArgument(file, "FileStore file");
			
			return new FileInputStream(file);
		}

		@Override
		public void writeFileObject(InputStream is, File file) throws IOException {
			Utilities.checkNotNullArgument(is, "InputStream");
			Utilities.checkNotNullArgument(file, "FileStore file");
			
			IOUtils.toFile(is, file);
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
