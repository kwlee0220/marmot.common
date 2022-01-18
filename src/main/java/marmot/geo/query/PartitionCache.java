package marmot.geo.query;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalNotification;

import marmot.MarmotRuntime;
import marmot.RecordSet;
import marmot.dataset.DataSet;
import marmot.protobuf.PBRecordProtos;
import utils.Utilities;
import utils.fostore.FileObjectHandler;
import utils.fostore.FileObjectStore;
import utils.func.FOption;
import utils.func.Unchecked;
import utils.io.IOUtils;
import utilsx.io.Lz4Compressions;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PartitionCache {
	private static final Logger s_logger = LoggerFactory.getLogger(PartitionCache.class);
	private static final long CACHE_EXPIRE_SECONDS = MINUTES.toSeconds(30);

	private final LoadingCache<String,DataSet> m_dsCache;
	private final FileObjectStore<PartitionKey,InputStream> m_fileCache;

	public PartitionCache(MarmotRuntime marmot, File storeRoot, long expireSecs)
		throws IOException {
		s_logger.info("use dataset_partition_cache: {}", storeRoot);
		
		m_fileCache = new FileObjectStore<>(storeRoot, new ParitionFileHandler(storeRoot));
		m_dsCache = CacheBuilder.newBuilder()
								.expireAfterAccess(expireSecs, SECONDS)
								.removalListener(this::onDataSetRemoved)
								.build(new CacheLoader<String,DataSet>() {
									@Override
									public DataSet load(String key) throws Exception {
										return marmot.getDataSet(key);
									}
								});
	}

	public PartitionCache(MarmotRuntime marmot, File storeRoot) throws IOException {
		this(marmot, storeRoot, CACHE_EXPIRE_SECONDS);
	}
	
	public boolean exists(String dsId, String quadKey) {
		return m_fileCache.exists(new PartitionKey(dsId, quadKey));
	}
	
	public RecordSet get(String dsId, String quadKey) throws IOException {
		InputStream is;
		
		// 오래된 데이터세트에 대한 파티션 파일이 삭제되게 하기 위해서
		// 먼저 dsId를 사용해서 m_dsCache를 접근한다.
		// 만일 일정기간동안 사용되지 않은 데이터세트의 파티션들이 m_fileCache에 있다면
		// 이때 제거된다.
		DataSet ds = Unchecked.getOrThrowRuntimeException(() -> m_dsCache.get(dsId));
		
		PartitionKey key = new PartitionKey(dsId, quadKey);
		FOption<InputStream> ois = m_fileCache.get(key);
		if ( ois.isPresent() ) {	// cache에 해당 파티션이 존재하는 경
			is = ois.getUnchecked();
		}
		else {	// cache에 파티션이 존재하지 않는 경우
			RecordSet cluster = ds.readSpatialCluster(quadKey);
			
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
	
	public void remove(String dsId, String quadKey) throws IOException {
		m_fileCache.remove(new PartitionKey(dsId, quadKey));
	}
	
	public Set<PartitionKey> keySet() throws IOException {
		return m_fileCache.getFileObjectKeyAll();
	}
	
	public File getTopDir() {
		return m_fileCache.getRootDir();
	}
	
	private File writeIntoCache(PartitionKey key, RecordSet rset)
		throws IOException {
		rset = RecordSet.from(rset.getRecordSchema(), rset.fstream()
						.shuffle());
		InputStream is = PBRecordProtos.toInputStream(rset); 
		try {
			is = Lz4Compressions.compress(is);
			return m_fileCache.insert(key, is);
		}
		finally {
			is.close();
		}
	}
	
	private void onDataSetRemoved(RemovalNotification<String,DataSet> noti) {
		String dsId = noti.getKey();
		
		s_logger.info("victim selected: dataset={}", dsId);
		try {
			m_fileCache.findFileObjectKeyAll(k -> k.m_dsId.equals(dsId))
						.forEach(Unchecked.ignore(m_fileCache::remove));
		}
		catch ( IOException ignored ) { }
	}
	
	public static final class PartitionKey {
		private final String m_dsId;
		private final String m_quadKey;
		
		PartitionKey(String dsId, String quadKey) {
			Utilities.checkNotNullArgument(dsId, "DataSet id");
			Utilities.checkNotNullArgument(quadKey, "quad-key");
			
			m_dsId = dsId;
			m_quadKey = quadKey;
		}
		
		public String getDataSetId() {
			return m_dsId;
		}
		
		public String getQuadKey() {
			return m_quadKey;
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
		private final String m_rootDirPath;
		private final int m_rootDirPathLength;
		
		ParitionFileHandler(File rootDir) {
			m_rootDir = rootDir;
			m_rootDirPath = rootDir.getAbsolutePath();
			m_rootDirPathLength = m_rootDirPath.length();
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
			String suffix = file.getAbsolutePath().substring(m_rootDirPathLength);
			int idx = suffix.lastIndexOf('/');
			if ( idx < 0 ) {
				throw new IllegalArgumentException("invalid File: " + file);
			}
			
			return new PartitionKey(suffix.substring(0, idx), file.getName());
		}

		@Override
		public boolean isVallidFile(File file) {
			return file.isFile() && file.getAbsolutePath().startsWith(m_rootDirPath);
		}
	}
}
