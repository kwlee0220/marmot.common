package marmot.geo.command;

import com.google.common.base.Preconditions;

import io.vavr.control.Option;
import utils.UnitUtils;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ClusterDataSetOptions {
	private Option<String> m_quadKeyFilePath = Option.none();
	private Option<Double> m_sampleRatio = Option.none();
	private Option<Double> m_blockFillRatio = Option.none();
	private Option<Long> m_blockSize = Option.none();
	private Option<Integer> m_workerCount = Option.none();
	
	public static ClusterDataSetOptions create() {
		return new ClusterDataSetOptions();
	}
	
	public Option<String> quadKeyFilePath() {
		return m_quadKeyFilePath;
	}
	
	public ClusterDataSetOptions quadKeyFilePath(Option<String> path) {
		m_quadKeyFilePath = path;
		return this;
	}
	
	public Option<Double> sampleRatio() {
		return m_sampleRatio;
	}
	
	public ClusterDataSetOptions sampleRatio(Option<Double> ratio) {
		if ( ratio.isDefined() ) {
			Preconditions.checkArgument(ratio.get() > 0, "invalid sample_ratio: value=" + ratio);
		}
		
		m_sampleRatio = ratio;
		return this;
	}
	
	public Option<Double> blockFillRatio() {
		return m_blockFillRatio;
	}
	
	public ClusterDataSetOptions blockFillRatio(Option<Double> ratio) {
		if ( ratio.isDefined() ) {
			Preconditions.checkArgument(ratio.get() > 0, "invalid block_fill_ratio: value=" + ratio);
		}
		
		m_blockFillRatio = ratio;
		return this;
	}
	
	public Option<Long> blockSize() {
		return m_blockSize;
	}
	
	public ClusterDataSetOptions blockSize(long blkSize) {
		Preconditions.checkArgument(blkSize > 0, "invalid block_size=" + blkSize);
		return blockSize(Option.some(blkSize));
	}
	
	public ClusterDataSetOptions blockSize(Option<Long> blkSize) {
		if ( blkSize.isDefined() ) {
			Preconditions.checkArgument(blkSize.get() > 0, "invalid block_size: value=" + blkSize);
		}
		
		m_blockSize = blkSize;
		return this;
	}
	
	public Option<Integer> workerCount() {
		return m_workerCount;
	}
	
	public ClusterDataSetOptions workerCount(int count) {
		Preconditions.checkArgument(count > 0, "invalid worker_count=" + count);
		return workerCount(Option.some(count));
	}
	
	public ClusterDataSetOptions workerCount(Option<Integer> count) {
		if ( count.isDefined() ) {
			Preconditions.checkArgument(count.get() > 0, "invalid worker_count: value=" + count);
		}
		
		m_workerCount = count;
		return this;
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		
		m_quadKeyFilePath.forEach(path -> builder.append(String.format("quad_keys=%s,", path)));
		m_sampleRatio.forEach(ratio -> builder.append(String.format("sampling=%.1f%%,", ratio*100)));
		m_blockFillRatio.forEach(ratio -> builder.append(String.format("fill_ratio=%.1f%%,", ratio*100)));
		m_blockSize.forEach(size -> builder.append(String.format("block=%s,",
													UnitUtils.toByteSizeString(size, "mb", "%.0f"))));
		m_workerCount.forEach(count -> builder.append(String.format("workers=%d,", count)));
		
		if ( builder.length() > 0 ) {
			builder.setLength(builder.length()-1);
		}
		
		return builder.toString();
	}
}
