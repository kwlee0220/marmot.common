package marmot.plan;

import marmot.proto.optor.GroupByKeyProto;
import marmot.support.PBSerializable;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Group implements PBSerializable<GroupByKeyProto> {
	private final String m_keys;
	private FOption<String> m_tagCols = FOption.empty();
	private FOption<String> m_orderBy = FOption.empty();
	private FOption<Integer> m_workerCount = FOption.empty();
	
	public static Group ofKeys(String keyCols) {
		return new Group(keyCols);
	}
	
	private Group(String keys) {
		m_keys = keys;
	}
	
	public String keys() {
		return m_keys;
	}
	
	public FOption<String> tags() {
		return m_tagCols;
	}
	
	public Group tags(String cols) {
		m_tagCols = FOption.of(cols);
		return this;
	}
	
	public Group withTags(String cols) {
		return tags(cols);
	}
	
	public FOption<String> orderBy() {
		return m_orderBy;
	}
	
	public Group orderBy(String cols) {
		m_orderBy = FOption.of(cols);
		return this;
	}
	
	public FOption<Integer> workerCount() {
		return m_workerCount;
	}
	
	public Group workerCount(int workerCount) {
		m_workerCount = FOption.of(workerCount);
		return this;
	}

	public static Group fromProto(GroupByKeyProto proto) {
		Group opts = Group.ofKeys(proto.getCompareColumns());
		
		switch ( proto.getOptionalTagColumnsCase() ) {
			case TAG_COLUMNS:
				opts.tags(proto.getTagColumns());
				break;
			default:
		}
		switch ( proto.getOptionalOrderColumnsCase() ) {
			case ORDER_COLUMNS:
				opts.orderBy(proto.getOrderColumns());
				break;
			default:
		}
		switch ( proto.getOptionalGroupWorkerCountCase() ) {
			case GROUP_WORKER_COUNT:
				opts.orderBy(proto.getOrderColumns());
				break;
			default:
		}
		
		return opts;
	}

	@Override
	public GroupByKeyProto toProto() {
		GroupByKeyProto.Builder builder = GroupByKeyProto.newBuilder()
															.setCompareColumns(m_keys);
		
		m_tagCols.ifPresent(builder::setTagColumns);
		m_orderBy.ifPresent(builder::setOrderColumns);
		m_workerCount.ifPresent(builder::setGroupWorkerCount);
		
		return builder.build();
	}
}
