package marmot.plan;

import marmot.proto.optor.LoadOptionsProto;
import marmot.support.PBSerializable;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class LoadOptions implements PBSerializable<LoadOptionsProto> {
	private FOption<Integer> m_nsplits = FOption.empty();
	
	public static LoadOptions create() {
		return new LoadOptions();
	}
	
	public LoadOptions splitCount(int count) {
		m_nsplits = FOption.of(count);
		return this;
	}
	
	public FOption<Integer> splitCount() {
		return m_nsplits;
	}
	
	public LoadOptions duplicate() {
		LoadOptions opts = LoadOptions.create();
		opts.m_nsplits = m_nsplits;
		
		return opts;
	}

	public static LoadOptions fromProto(LoadOptionsProto proto) {
		LoadOptions opts = LoadOptions.create();
		
		switch ( proto.getOptionalSplitCountPerBlockCase() ) {
			case SPLIT_COUNT_PER_BLOCK:
				opts.splitCount(proto.getSplitCountPerBlock());
			default:
		}
		
		return opts;
	}

	@Override
	public LoadOptionsProto toProto() {
		LoadOptionsProto.Builder builder = LoadOptionsProto.newBuilder();
		
		m_nsplits.ifPresent(builder::setSplitCountPerBlock);
		
		return builder.build();
	}

}
