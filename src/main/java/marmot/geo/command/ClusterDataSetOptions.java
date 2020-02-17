package marmot.geo.command;

import com.google.common.base.Preconditions;

import marmot.proto.service.ClusterDataSetOptionsProto;
import marmot.support.PBSerializable;
import utils.UnitUtils;
import utils.Utilities;
import utils.func.FOption;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ClusterDataSetOptions implements PBSerializable<ClusterDataSetOptionsProto> {
	private static final ClusterDataSetOptions EMPTY
									= new ClusterDataSetOptions(FOption.empty(), FOption.empty(),
																FOption.empty(), FOption.empty());
	
	private final FOption<Double> m_sampleRatio;
	private final FOption<Long> m_clusterSize;
	private final FOption<Long> m_blockSize;
	private final FOption<Integer> m_workerCount;
	
	private ClusterDataSetOptions(FOption<Double> sampleRatio, FOption<Long> clusterSize,
									FOption<Long> blockSize, FOption<Integer> workerCount) {
		m_sampleRatio = sampleRatio;
		m_clusterSize = clusterSize;
		m_blockSize = blockSize;
		m_workerCount = workerCount;
	}
	
	public static ClusterDataSetOptions DEFAULT() {
		return EMPTY;
	}
	
	public static ClusterDataSetOptions WORKER_COUNT(int count) {
		Utilities.checkArgument(count > 0, "count > 0");
		
		return new ClusterDataSetOptions(FOption.empty(),
									FOption.empty(), FOption.empty(), FOption.of(count));
	}
	
	public FOption<Double> sampleRatio() {
		return m_sampleRatio;
	}
	
	public ClusterDataSetOptions sampleRatio(double ratio) {
		Preconditions.checkArgument(ratio > 0, "invalid sample_ratio: value=" + ratio);
		
		return new ClusterDataSetOptions(FOption.of(ratio), m_clusterSize, m_blockSize, m_workerCount);
	}
	
	public FOption<Long> clusterSize() {
		return m_clusterSize;
	}
	
	public ClusterDataSetOptions clusterSize(long size) {
		Preconditions.checkArgument(size > 0, "invalid cluster_size=" + size);
		
		return new ClusterDataSetOptions(m_sampleRatio, FOption.of(size), m_blockSize, m_workerCount);
	}
	
	public FOption<Long> blockSize() {
		return m_blockSize;
	}
	
	public ClusterDataSetOptions blockSize(long blockSize) {
		Preconditions.checkArgument(blockSize > 0, "invalid block_size=" + blockSize);
		
		return new ClusterDataSetOptions(m_sampleRatio, m_clusterSize, FOption.of(blockSize),
											m_workerCount);
	}
	
	public FOption<Integer> workerCount() {
		return m_workerCount;
	}
	
	public ClusterDataSetOptions workerCount(int count) {
		Preconditions.checkArgument(count > 0, "invalid worker_count=" + count);
		
		return new ClusterDataSetOptions(m_sampleRatio, m_clusterSize, m_blockSize, FOption.of(count));
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		
		m_sampleRatio.ifPresent(ratio -> builder.append(String.format("sampling=%.1f%%,", ratio*100)));
		m_clusterSize.ifPresent(size -> builder.append(String.format("cluster_size=%s,",
													UnitUtils.toByteSizeString(size, "mb", "%.0f"))));
		m_blockSize.ifPresent(size -> builder.append(String.format("block=%s,",
													UnitUtils.toByteSizeString(size, "mb", "%.0f"))));
		m_workerCount.ifPresent(count -> builder.append(String.format("workers=%d,", count)));
		
		if ( builder.length() > 0 ) {
			builder.setLength(builder.length()-1);
		}
		
		return builder.toString();
	}
	
	public static ClusterDataSetOptions fromProto(ClusterDataSetOptionsProto proto) {
		ClusterDataSetOptions opts = ClusterDataSetOptions.DEFAULT();
		
		switch ( proto.getOptionalSampleRatioCase() ) {
			case SAMPLE_RATIO:
				opts = opts.sampleRatio(proto.getSampleRatio());
				break;
			case OPTIONALSAMPLERATIO_NOT_SET:
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
		
		switch ( proto.getOptionalWorkerCountCase() ) {
			case WORKER_COUNT:
				opts = opts.workerCount(proto.getWorkerCount());
				break;
			case OPTIONALWORKERCOUNT_NOT_SET:
				break;
			default:
				throw new AssertionError();
		}
		
		return opts;
	}

	@Override
	public ClusterDataSetOptionsProto toProto() {
		ClusterDataSetOptionsProto.Builder builder = ClusterDataSetOptionsProto.newBuilder();
		m_sampleRatio.ifPresent(builder::setSampleRatio);
		m_clusterSize.ifPresent(builder::setClusterSize);
		m_blockSize.ifPresent(builder::setBlockSize);
		m_workerCount.ifPresent(builder::setWorkerCount);
		
		return builder.build();
	}
}
