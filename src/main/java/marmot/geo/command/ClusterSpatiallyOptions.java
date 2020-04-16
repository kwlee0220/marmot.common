package marmot.geo.command;

import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;

import javax.annotation.Nullable;

import com.google.common.collect.Lists;
import com.vividsolutions.jts.geom.Envelope;

import marmot.proto.service.ClusterSpatiallyOptionsProto;
import marmot.protobuf.PBUtils;
import marmot.support.PBSerializable;
import utils.CSV;
import utils.UnitUtils;
import utils.Utilities;
import utils.func.FOption;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ClusterSpatiallyOptions implements PBSerializable<ClusterSpatiallyOptionsProto>,
												Serializable {
	private static final long serialVersionUID = 1L;

	public static final ClusterSpatiallyOptions DEFAULT
				= new ClusterSpatiallyOptions(false, FOption.empty(), FOption.empty(), null, null,
											-1, FOption.empty(), FOption.empty(), FOption.empty());
	public static final ClusterSpatiallyOptions FORCE = DEFAULT.force(true);

	private final boolean m_force;						// create file
	private final FOption<Integer> m_mapperCount;
	private final FOption<Envelope> m_validRange;
	@Nullable private final List<String> m_quadKeyList;	// 셋 중 최대 하나만 의미 있는 값을 가짐
	@Nullable private final String m_quadKeyDsId;
	@Nullable private final long m_sampleSize;			
	private final FOption<Integer> m_partitionCount;
	private final FOption<Long> m_clusterSize;
	private final FOption<Long> m_blockSize;
	
	private ClusterSpatiallyOptions(boolean force, FOption<Integer> mapperCount,
									FOption<Envelope> validRange, Collection<String> quadKeyList,
									String quadKeyDsId, long sampleSize, FOption<Integer> paritionCount,
									FOption<Long> clusterSize, FOption<Long> blockSize) {
		m_force = force;
		m_mapperCount = mapperCount;
		m_validRange = validRange;
		m_quadKeyList = quadKeyList != null ? Lists.newArrayList(quadKeyList) : null;
		m_quadKeyDsId = quadKeyDsId;
		m_sampleSize = sampleSize;
		m_partitionCount = paritionCount;
		m_clusterSize = clusterSize;
		m_blockSize = blockSize;
	}
	
	public boolean force() {
		return m_force;
	}
	public ClusterSpatiallyOptions force(boolean flag) {
		return new ClusterSpatiallyOptions(flag, m_mapperCount, m_validRange, m_quadKeyList,
											m_quadKeyDsId, m_sampleSize, m_partitionCount,
											m_clusterSize, m_blockSize);
	}
	
	public FOption<Integer> mapperCount() {
		return m_mapperCount;
	}
	public ClusterSpatiallyOptions mapperCount(FOption<Integer> mapperCount) {
		return new ClusterSpatiallyOptions(m_force, mapperCount, m_validRange, m_quadKeyList,
											m_quadKeyDsId, m_sampleSize, m_partitionCount,
											m_clusterSize, m_blockSize);
	}
	public ClusterSpatiallyOptions mapperCount(int mapperCount) {
		return mapperCount(FOption.of(mapperCount));
	}
	
	public FOption<Envelope> validRange() {
		return m_validRange;
	}
	public ClusterSpatiallyOptions validRange(FOption<Envelope> validRange) {
		return new ClusterSpatiallyOptions(m_force, m_mapperCount, validRange, m_quadKeyList,
											m_quadKeyDsId, m_sampleSize, m_partitionCount,
											m_clusterSize, m_blockSize);
	}
	public ClusterSpatiallyOptions validRange(Envelope validRange) {
		Utilities.checkNotNullArgument(validRange, "valid_range");
		
		return validRange(FOption.of(validRange));
	}
	
	public FOption<List<String>> quadKeyList() {
		return FOption.ofNullable(m_quadKeyList);
	}
	public ClusterSpatiallyOptions quadKeyList(Collection<String> quadKeyList) {
		Utilities.checkArgument(quadKeyList != null && quadKeyList.size() > 0,
								"invalid quadKeyList=" + quadKeyList);

		return new ClusterSpatiallyOptions(m_force, m_mapperCount, m_validRange, quadKeyList,
											null, -1, m_partitionCount,
											m_clusterSize, m_blockSize);
	}
	
	public FOption<String> quadKeyDsId() {
		return FOption.ofNullable(m_quadKeyDsId);
	}
	public ClusterSpatiallyOptions quadKeyDsId(String quadKeyDsId) {
		Utilities.checkNotNullArgument(quadKeyDsId, "quadKeyDsId");

		return new ClusterSpatiallyOptions(m_force, m_mapperCount, m_validRange, null,
											quadKeyDsId, -1, m_partitionCount,
											m_clusterSize, m_blockSize);
	}
	
	public FOption<Long> sampleSize() {
		return (m_sampleSize > 0) ? FOption.of(m_sampleSize) : FOption.empty();
	}
	public ClusterSpatiallyOptions sampleSize(long sampleSize) {
		Utilities.checkArgument(sampleSize > 0, "invalid sampleSize=" + sampleSize);

		return new ClusterSpatiallyOptions(m_force, m_mapperCount, m_validRange, null,
											null, sampleSize, m_partitionCount,
											m_clusterSize, m_blockSize);
	}
	
	public FOption<Integer> partitionCount() {
		return m_partitionCount;
	}
	public ClusterSpatiallyOptions partitionCount(int count) {
		Utilities.checkArgument(count > 0, "invalid partition_count=" + count);
		
		return partitionCount(FOption.of(count));
	}
	public ClusterSpatiallyOptions partitionCount(FOption<Integer> count) {
		return new ClusterSpatiallyOptions(m_force, m_mapperCount, m_validRange, m_quadKeyList,
											m_quadKeyDsId, m_sampleSize, count,
											m_clusterSize, m_blockSize);
	}
	
	public FOption<Long> clusterSize() {
		return m_clusterSize;
	}
	public ClusterSpatiallyOptions clusterSize(FOption<Long> clusterSize) {
		return new ClusterSpatiallyOptions(m_force, m_mapperCount, m_validRange, m_quadKeyList,
											m_quadKeyDsId, m_sampleSize, m_partitionCount,
											clusterSize, m_blockSize);
	}
	public ClusterSpatiallyOptions clusterSize(long clusterSize) {
		Utilities.checkArgument(clusterSize > 0, "invalid maxClusterSize=" + clusterSize);
		
		return clusterSize(FOption.of(clusterSize));
	}
	
	public FOption<Long> blockSize() {
		return m_blockSize;
	}
	public ClusterSpatiallyOptions blockSize(FOption<Long> blockSize) {
		return new ClusterSpatiallyOptions(m_force, m_mapperCount, m_validRange, m_quadKeyList,
											m_quadKeyDsId, m_sampleSize, m_partitionCount,
											m_clusterSize, blockSize);
	}
	public ClusterSpatiallyOptions blockSize(long blockSize) {
		Utilities.checkArgument(blockSize > 0, "invalid block_size=" + blockSize);
		
		return blockSize(FOption.of(blockSize));
	}
	
	public EstimateQuadKeysOptions toEstimateQuadKeysOptions() {
		EstimateQuadKeysOptions opts = EstimateQuadKeysOptions.DEFAULT();
		opts = m_mapperCount.transform(opts, (o,c) -> o.mapperCount(c));
		opts = m_validRange.transform(opts, (o,r) -> o.validRange(r));
		opts = m_clusterSize.transform(opts, (o,r) -> o.clusterSize(r));
		
		return opts;
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();

		m_mapperCount.ifPresent(count -> builder.append(String.format("mappers=%d,", count)));
		if ( m_quadKeyList != null ) {
			builder.append("quad_keys=" + FStream.from(m_quadKeyList).join(',').substring(0, 20) + ",");
		}
		else if ( m_quadKeyDsId != null ) {
			builder.append("quadkey_ds=" + m_quadKeyDsId + ",");
		}
		else if ( m_sampleSize > 0 ) {
			builder.append("sample_size=" + UnitUtils.toByteSizeString(m_sampleSize) + ",");
		}
		m_clusterSize.ifPresent(size -> builder.append(String.format("cluster_size=%s,",
								UnitUtils.toByteSizeString(size))));
		m_blockSize.ifPresent(size -> builder.append(String.format("block=%s,",
													UnitUtils.toByteSizeString(size))));
		m_partitionCount.ifPresent(count -> builder.append(String.format("partitions=%d,", count)));
		
		if ( builder.length() > 0 ) {
			builder.setLength(builder.length()-1);
		}
		
		return builder.toString();
	}
	
	private Object writeReplace() {
		return new SerializationProxy(this);
	}
	
	private void readObject(ObjectInputStream stream) throws InvalidObjectException {
		throw new InvalidObjectException("Use Serialization Proxy instead.");
	}

	private static class SerializationProxy implements Serializable {
		private static final long serialVersionUID = 1L;
		
		private final ClusterSpatiallyOptionsProto m_proto;
		
		private SerializationProxy(ClusterSpatiallyOptions opts) {
			m_proto = opts.toProto();
		}
		
		private Object readResolve() {
			return ClusterSpatiallyOptions.fromProto(m_proto);
		}
	}
	
	public static ClusterSpatiallyOptions fromProto(ClusterSpatiallyOptionsProto proto) {
		ClusterSpatiallyOptions opts = ClusterSpatiallyOptions.DEFAULT
															.force(proto.getForce());
		
		switch ( proto.getOptionalMapperCountCase() ) {
			case MAPPER_COUNT:
				opts = opts.mapperCount(proto.getMapperCount());
				break;
			case OPTIONALMAPPERCOUNT_NOT_SET:
				break;
			default:
				throw new AssertionError();
		}
		
		switch ( proto.getOptionalValidRangeCase() ) {
			case VALID_RANGE:
				opts = opts.validRange(PBUtils.fromProto(proto.getValidRange()));
				break;
			case OPTIONALVALIDRANGE_NOT_SET:
				break;
			default:
				throw new AssertionError();
		}
		
		switch ( proto.getEitherQuadKeyInfoCase() ) {
			case QUAD_KEY_LIST:
				opts = opts.quadKeyList(CSV.parseCsv(proto.getQuadKeyList()).toList());
				break;
			case QUAD_KEY_DS_ID:
				opts = opts.quadKeyDsId(proto.getQuadKeyDsId());
				break;
			case SAMPLE_SIZE:
				opts = opts.sampleSize(proto.getSampleSize());
				break;
			case EITHERQUADKEYINFO_NOT_SET:
				break;
			default:
				throw new AssertionError();
		}
		
		switch ( proto.getOptionalPartitionCountCase() ) {
			case PARTITION_COUNT:
				opts = opts.partitionCount(proto.getPartitionCount());
				break;
			case OPTIONALPARTITIONCOUNT_NOT_SET:
				break;
			default:
				throw new AssertionError();
		}
		
		switch ( proto.getOptionalClusterSizeCase() ) {
			case CLUSTER_SIZE:
				opts = opts.clusterSize(proto.getClusterSize());
				break;
			case OPTIONALCLUSTERSIZE_NOT_SET:
				break;
			default:
				throw new AssertionError();
		}
		
		switch ( proto.getOptionalBlockSizeCase() ) {
			case BLOCK_SIZE:
				opts = opts.blockSize(proto.getBlockSize());
				break;
			case OPTIONALBLOCKSIZE_NOT_SET:
				break;
			default:
				throw new AssertionError();
		}
		
		return opts;
	}

	@Override
	public ClusterSpatiallyOptionsProto toProto() {
		ClusterSpatiallyOptionsProto.Builder builder = ClusterSpatiallyOptionsProto.newBuilder()
																			.setForce(m_force);
		m_mapperCount.ifPresent(builder::setMapperCount);
		m_validRange.map(PBUtils::toProto).ifPresent(builder::setValidRange);
		if ( m_quadKeyList != null ) {
			builder.setQuadKeyList(FStream.from(m_quadKeyList).join(','));
		}
		else if ( m_quadKeyDsId != null ) {
			builder.setQuadKeyDsId(m_quadKeyDsId);
		}
		else if ( m_sampleSize > 0 ) {
			builder.setSampleSize(m_sampleSize);
		}
		m_partitionCount.ifPresent(builder::setPartitionCount);
		m_clusterSize.ifPresent(builder::setClusterSize);
		m_blockSize.ifPresent(builder::setBlockSize);
		
		return builder.build();
	}
}
