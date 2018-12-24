package marmot.geo.command;

import com.google.common.base.Preconditions;

import utils.UnitUtils;
import utils.func.FOption;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ClusterDataSetOptions {
	private FOption<String> m_quadKeyFilePath = FOption.empty();
	private FOption<Double> m_sampleRatio = FOption.empty();
	private FOption<Double> m_blockFillRatio = FOption.empty();
	private FOption<Long> m_blockSize = FOption.empty();
	private FOption<Integer> m_workerCount = FOption.empty();
	
	public static ClusterDataSetOptions create() {
		return new ClusterDataSetOptions();
	}
	
	public FOption<String> quadKeyFilePath() {
		return m_quadKeyFilePath;
	}
	
	public ClusterDataSetOptions quadKeyFilePath(FOption<String> path) {
		m_quadKeyFilePath = path;
		return this;
	}
	
	public FOption<Double> sampleRatio() {
		return m_sampleRatio;
	}
	
	public ClusterDataSetOptions sampleRatio(FOption<Double> ratio) {
		if ( ratio.isPresent() ) {
			Preconditions.checkArgument(ratio.get() > 0, "invalid sample_ratio: value=" + ratio);
		}
		
		m_sampleRatio = ratio;
		return this;
	}
	
	public ClusterDataSetOptions sampleRatio(double ratio) {
		Preconditions.checkArgument(ratio > 0, "invalid sample_ratio: value=" + ratio);
		
		m_sampleRatio = FOption.of(ratio);
		return this;
	}
	
	public FOption<Double> blockFillRatio() {
		return m_blockFillRatio;
	}
	
	public ClusterDataSetOptions blockFillRatio(FOption<Double> ratio) {
		if ( ratio.isPresent() ) {
			Preconditions.checkArgument(ratio.get() > 0, "invalid block_fill_ratio: value=" + ratio);
		}
		
		m_blockFillRatio = ratio;
		return this;
	}
	
	public ClusterDataSetOptions blockFillRatio(double ratio) {
		Preconditions.checkArgument(ratio > 0, "invalid block_fill_ratio: value=" + ratio);
		
		m_blockFillRatio = FOption.of(ratio);
		return this;
	}
	
	public FOption<Long> blockSize() {
		return m_blockSize;
	}
	
	public ClusterDataSetOptions blockSize(long blkSize) {
		Preconditions.checkArgument(blkSize > 0, "invalid block_size=" + blkSize);
		return blockSize(FOption.of(blkSize));
	}
	
	public ClusterDataSetOptions blockSize(FOption<Long> blkSize) {
		if ( blkSize.isPresent() ) {
			Preconditions.checkArgument(blkSize.get() > 0, "invalid block_size: value=" + blkSize);
		}
		
		m_blockSize = blkSize;
		return this;
	}
	
	public FOption<Integer> workerCount() {
		return m_workerCount;
	}
	
	public ClusterDataSetOptions workerCount(int count) {
		Preconditions.checkArgument(count > 0, "invalid worker_count=" + count);
		return workerCount(FOption.of(count));
	}
	
	public ClusterDataSetOptions workerCount(FOption<Integer> count) {
		if ( count.isPresent() ) {
			Preconditions.checkArgument(count.get() > 0, "invalid worker_count: value=" + count);
		}
		
		m_workerCount = count;
		return this;
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
}
