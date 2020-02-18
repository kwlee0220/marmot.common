package marmot.geo.command;

import com.google.common.base.Preconditions;

import marmot.proto.service.CreateSpatialIndexOptionsProto;
import marmot.support.PBSerializable;
import utils.UnitUtils;
import utils.Utilities;
import utils.func.FOption;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CreateSpatialIndexOptions implements PBSerializable<CreateSpatialIndexOptionsProto> {
	private static final CreateSpatialIndexOptions EMPTY
									= new CreateSpatialIndexOptions(FOption.empty(), FOption.empty(),
																FOption.empty(), FOption.empty());
	
	private final FOption<Double> m_sampleRatio;
	private final FOption<Long> m_clusterSize;
	private final FOption<Long> m_blockSize;
	private final FOption<Integer> m_workerCount;
	
	private CreateSpatialIndexOptions(FOption<Double> sampleRatio, FOption<Long> clusterSize,
									FOption<Long> blockSize, FOption<Integer> workerCount) {
		m_sampleRatio = sampleRatio;
		m_clusterSize = clusterSize;
		m_blockSize = blockSize;
		m_workerCount = workerCount;
	}
	
	public static CreateSpatialIndexOptions DEFAULT() {
		return EMPTY;
	}
	
	public static CreateSpatialIndexOptions WORKER_COUNT(int count) {
		Utilities.checkArgument(count > 0, "count > 0");
		
		return new CreateSpatialIndexOptions(FOption.empty(),
									FOption.empty(), FOption.empty(), FOption.of(count));
	}
	
	public FOption<Double> sampleRatio() {
		return m_sampleRatio;
	}
	
	public CreateSpatialIndexOptions sampleRatio(double ratio) {
		Preconditions.checkArgument(ratio > 0, "invalid sample_ratio: value=" + ratio);
		
		return new CreateSpatialIndexOptions(FOption.of(ratio), m_clusterSize, m_blockSize, m_workerCount);
	}
	
	public FOption<Long> clusterSize() {
		return m_clusterSize;
	}
	
	public CreateSpatialIndexOptions clusterSize(long size) {
		Preconditions.checkArgument(size > 0, "invalid cluster_size=" + size);
		
		return new CreateSpatialIndexOptions(m_sampleRatio, FOption.of(size), m_blockSize, m_workerCount);
	}
	
	public FOption<Long> blockSize() {
		return m_blockSize;
	}
	
	public CreateSpatialIndexOptions blockSize(long blockSize) {
		Preconditions.checkArgument(blockSize > 0, "invalid block_size=" + blockSize);
		
		return new CreateSpatialIndexOptions(m_sampleRatio, m_clusterSize, FOption.of(blockSize),
											m_workerCount);
	}
	
	public FOption<Integer> workerCount() {
		return m_workerCount;
	}
	
	public CreateSpatialIndexOptions workerCount(int count) {
		Preconditions.checkArgument(count > 0, "invalid worker_count=" + count);
		
		return new CreateSpatialIndexOptions(m_sampleRatio, m_clusterSize, m_blockSize, FOption.of(count));
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
	
	public static CreateSpatialIndexOptions fromProto(CreateSpatialIndexOptionsProto proto) {
		CreateSpatialIndexOptions opts = CreateSpatialIndexOptions.DEFAULT();
		
		switch ( proto.getOptionalSampleRatioCase() ) {
			case SAMPLE_RATIO:
				opts = opts.sampleRatio(proto.getSampleRatio());
				break;
			case OPTIONALSAMPLERATIO_NOT_SET:
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
	public CreateSpatialIndexOptionsProto toProto() {
		CreateSpatialIndexOptionsProto.Builder builder = CreateSpatialIndexOptionsProto.newBuilder();
		m_sampleRatio.ifPresent(builder::setSampleRatio);
		m_blockSize.ifPresent(builder::setBlockSize);
		m_workerCount.ifPresent(builder::setWorkerCount);
		
		return builder.build();
	}
}
