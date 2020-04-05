package marmot.plan;

import marmot.proto.optor.LoadOptionsProto;
import marmot.support.PBSerializable;
import utils.Utilities;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class LoadOptions implements PBSerializable<LoadOptionsProto> {
	public static final LoadOptions DEFAULT = new LoadOptions(FOption.empty(), FOption.empty());
	
	private FOption<Integer> m_nsplits = FOption.empty();
	private FOption<Integer> m_mapperCount = FOption.empty();
	
	private LoadOptions(FOption<Integer> splitCount, FOption<Integer> mapperCount) {
		m_nsplits = splitCount;
		m_mapperCount = mapperCount;
	}
	
	public static LoadOptions SPLIT_COUNT(int count) {
		return new LoadOptions(FOption.of(count), FOption.empty());
	}
	
	public static LoadOptions MAPPERS(int count) {
		Utilities.checkArgument(count >= 0, "invalid mapper count: " + count);
		
		return new LoadOptions(FOption.empty(), FOption.of(count));
	}
	
	public static LoadOptions FIXED_MAPPERS() {
		return new LoadOptions(FOption.empty(), FOption.of(0));
	}
	
	public FOption<Integer> splitCount() {
		return m_nsplits;
	}
	
	public FOption<Integer> mapperCount() {
		return m_mapperCount;
	}
	
	public LoadOptions splitCount(int count) {
		return new LoadOptions(FOption.of(count), m_mapperCount);
	}
	
	public LoadOptions mapperCount(int count) {
		return new LoadOptions(m_nsplits, FOption.of(count));
	}

	public static LoadOptions fromProto(LoadOptionsProto proto) {
		LoadOptions opts = DEFAULT;
		
		switch ( proto.getOptionalSplitCountPerBlockCase() ) {
			case SPLIT_COUNT_PER_BLOCK:
				opts = opts.splitCount(proto.getSplitCountPerBlock());
				break;
			case OPTIONALSPLITCOUNTPERBLOCK_NOT_SET:
				break;
			default:
				throw new AssertionError();
		}
		
		switch ( proto.getOptionalMapperCountCase() ) {
			case MAPPER_COUNT:
				opts = opts.mapperCount(proto.getMapperCount());
				break;
			case OPTIONALMAPPERCOUNT_NOT_SET:
				break;
			default:
				throw new AssertionError();
		}
		
		return opts;
	}

	@Override
	public LoadOptionsProto toProto() {
		LoadOptionsProto.Builder builder = LoadOptionsProto.newBuilder();
		
		m_nsplits.ifPresent(builder::setSplitCountPerBlock);
		m_mapperCount.ifPresent(builder::setMapperCount);
		
		return builder.build();
	}

}
