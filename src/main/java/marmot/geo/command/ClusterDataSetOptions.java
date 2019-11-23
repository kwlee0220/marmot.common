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
													FOption.empty(), FOption.empty(),
													FOption.empty());
	
	private final FOption<String> m_quadKeyFilePath;
	private final FOption<Double> m_sampleRatio;
	private final FOption<Double> m_blockFillRatio;
	private final FOption<Long> m_blockSize;
	private final FOption<Integer> m_workerCount;
	
	private ClusterDataSetOptions(FOption<String> path, FOption<Double> sample,
								FOption<Double> blockFill, FOption<Long> blkSz,
								FOption<Integer> cnt) {
		m_quadKeyFilePath = path;
		m_sampleRatio = sample;
		m_blockFillRatio = blockFill;
		m_blockSize = blkSz;
		m_workerCount = cnt;
	}
	
	public static ClusterDataSetOptions DEFAULT() {
		return EMPTY;
	}
	
	public static ClusterDataSetOptions WORKER_COUNT(int count) {
		Utilities.checkArgument(count > 0, "count > 0");
		
		return new ClusterDataSetOptions(FOption.empty(), FOption.empty(),
									FOption.empty(), FOption.empty(), FOption.of(count));
	}
	
	public FOption<String> quadKeyFilePath() {
		return m_quadKeyFilePath;
	}
	
	public ClusterDataSetOptions quadKeyFilePath(String path) {
		return new ClusterDataSetOptions(FOption.of(path), m_sampleRatio, m_blockFillRatio,
										m_blockSize, m_workerCount);
	}
	
	public FOption<Double> sampleRatio() {
		return m_sampleRatio;
	}
	
	public ClusterDataSetOptions sampleRatio(double ratio) {
		Preconditions.checkArgument(ratio > 0, "invalid sample_ratio: value=" + ratio);
		
		return new ClusterDataSetOptions(m_quadKeyFilePath, FOption.of(ratio),
										m_blockFillRatio, m_blockSize, m_workerCount);
	}
	
	public FOption<Double> blockFillRatio() {
		return m_blockFillRatio;
	}
	
	public ClusterDataSetOptions blockFillRatio(double ratio) {
		Preconditions.checkArgument(ratio > 0, "invalid block_fill_ratio: value=" + ratio);
		
		return new ClusterDataSetOptions(m_quadKeyFilePath, m_sampleRatio, FOption.of(ratio),
										m_blockSize, m_workerCount);
	}
	
	public FOption<Long> blockSize() {
		return m_blockSize;
	}
	
	public ClusterDataSetOptions blockSize(long blkSize) {
		Preconditions.checkArgument(blkSize > 0, "invalid block_size=" + blkSize);
		
		return new ClusterDataSetOptions(m_quadKeyFilePath, m_sampleRatio,
										m_blockFillRatio, FOption.of(blkSize), m_workerCount);
	}
	
	public FOption<Integer> workerCount() {
		return m_workerCount;
	}
	
	public ClusterDataSetOptions workerCount(int count) {
		Preconditions.checkArgument(count > 0, "invalid worker_count=" + count);
		
		return new ClusterDataSetOptions(m_quadKeyFilePath, m_sampleRatio,
										m_blockFillRatio, m_blockSize, FOption.of(count));
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		
		m_quadKeyFilePath.ifPresent(path -> builder.append(String.format("quad_keys=%s,", path)));
		m_sampleRatio.ifPresent(ratio -> builder.append(String.format("sampling=%.1f%%,", ratio*100)));
		m_blockFillRatio.ifPresent(ratio -> builder.append(String.format("fill_ratio=%.1f%%,", ratio*100)));
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
		
		switch ( proto.getOptionalQuadKeyFileCase() ) {
			case QUAD_KEY_FILE:
				opts = opts.quadKeyFilePath(proto.getQuadKeyFile());
				break;
			case OPTIONALQUADKEYFILE_NOT_SET:
				break;
			default:
				throw new AssertionError();
		}
		
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
		
		switch ( proto.getOptionalBlockFillRatioCase() ) {
			case BLOCK_FILL_RATIO:
				opts = opts.blockFillRatio(proto.getBlockFillRatio());
				break;
			case OPTIONALBLOCKFILLRATIO_NOT_SET:
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
		m_quadKeyFilePath.ifPresent(builder::setQuadKeyFile);
		m_sampleRatio.ifPresent(builder::setSampleRatio);
		m_blockSize.ifPresent(builder::setBlockSize);
		m_blockFillRatio.ifPresent(builder::setBlockFillRatio);
		m_workerCount.ifPresent(builder::setWorkerCount);
		
		return builder.build();
	}
}
