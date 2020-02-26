package marmot.geo.command;

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
																	FOption.empty());
	
	private final FOption<Long> m_sampleSize;
	private final FOption<Long> m_blockSize;
	private final FOption<Integer> m_workerCount;
	
	private CreateSpatialIndexOptions(FOption<Long> sampleSize, FOption<Long> blockSize,
											FOption<Integer> workerCount) {
		m_sampleSize = sampleSize;
		m_blockSize = blockSize;
		m_workerCount = workerCount;
	}
	
	public static CreateSpatialIndexOptions DEFAULT() {
		return EMPTY;
	}
	
	public static CreateSpatialIndexOptions WORKER_COUNT(int count) {
		Utilities.checkArgument(count > 0, "count > 0");
		
		return new CreateSpatialIndexOptions(FOption.empty(), FOption.empty(), FOption.of(count));
	}
	
	public FOption<Long> sampleSize() {
		return m_sampleSize;
	}
	
	public CreateSpatialIndexOptions sampleSize(long size) {
		Utilities.checkArgument(size > 0, "invalid sample_size=" + size);
		
		return new CreateSpatialIndexOptions(FOption.of(size), m_blockSize, m_workerCount);
	}
	
	public FOption<Long> blockSize() {
		return m_blockSize;
	}
	
	public CreateSpatialIndexOptions blockSize(long blockSize) {
		Utilities.checkArgument(blockSize > 0, "invalid block_size=" + blockSize);
		
		return new CreateSpatialIndexOptions(m_sampleSize, FOption.of(blockSize), m_workerCount);
	}
	
	public FOption<Integer> workerCount() {
		return m_workerCount;
	}
	
	public CreateSpatialIndexOptions workerCount(int count) {
		Utilities.checkArgument(count > 0, "invalid worker_count=" + count);
		
		return new CreateSpatialIndexOptions(m_sampleSize, m_blockSize, FOption.of(count));
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		
		m_sampleSize.ifPresent(size -> builder.append(String.format("sample_size=%s,",
													UnitUtils.toByteSizeString(size))));
		m_blockSize.ifPresent(size -> builder.append(String.format("block=%s,",
													UnitUtils.toByteSizeString(size))));
		m_workerCount.ifPresent(count -> builder.append(String.format("workers=%d,", count)));
		
		if ( builder.length() > 0 ) {
			builder.setLength(builder.length()-1);
		}
		
		return builder.toString();
	}
	
	public static CreateSpatialIndexOptions fromProto(CreateSpatialIndexOptionsProto proto) {
		CreateSpatialIndexOptions opts = CreateSpatialIndexOptions.DEFAULT();
		
		switch ( proto.getOptionalSampleSizeCase() ) {
			case SAMPLE_SIZE:
				opts = opts.sampleSize(proto.getSampleSize());
				break;
			case OPTIONALSAMPLESIZE_NOT_SET:
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
		m_sampleSize.ifPresent(builder::setSampleSize);
		m_blockSize.ifPresent(builder::setBlockSize);
		m_workerCount.ifPresent(builder::setWorkerCount);
		
		return builder.build();
	}
}
