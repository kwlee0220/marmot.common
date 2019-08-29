package marmot.plan;

import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;

import marmot.proto.optor.GroupByKeyProto;
import marmot.support.PBSerializable;
import utils.CSV;
import utils.KeyValue;
import utils.Utilities;
import utils.func.FOption;
import utils.func.Funcs;
import utils.stream.FStream;

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
	
	public static Group parseGroup(String expr) {
		Utilities.checkNotNullArgument(expr, "Group string rep is null");
	
		Map<String,String> kvMap = CSV.parseCsv(expr, ';')
									.map(Group::parseKeyValue)
									.toMap(KeyValue::key, KeyValue::value);
		String keyCols = kvMap.get("keys");
		if ( keyCols == null ) {
			throw new IllegalArgumentException("'keys' is not present: expr=" + expr);
		}
		
		Group group = Group.ofKeys(keyCols);
		Funcs.acceptIfPresent(kvMap, "tags", (k,v) -> group.tags(v));
		Funcs.acceptIfPresent(kvMap, "orderBy", (k,v) -> group.orderBy(v));
		Funcs.acceptIfPresent(kvMap, "workers", (k,v) -> group.workerCount(Integer.parseInt(v)));
		
		return group;
	}
	
	private static KeyValue<String,String> parseKeyValue(String expr) {
		List<String> parts = CSV.parseCsv(expr, '=')
								.map(String::trim)
								.toList();
		if ( parts.size() != 2 ) {
			throw new IllegalArgumentException("invalid key-value: " + expr);
		}
		
		return KeyValue.of(parts.get(0), parts.get(1));
	}
	
	public String toString() {
		List<KeyValue<String,String>> kvList = Lists.newArrayList();
		kvList.add(KeyValue.of("keys", m_keys));
		m_tagCols.ifPresent(cols -> kvList.add(KeyValue.of("tags", cols)));
		m_orderBy.ifPresent(cols -> kvList.add(KeyValue.of("orderBy", cols)));
		m_workerCount.map(cnt -> "" + cnt)
					.ifPresent(cnt -> kvList.add(KeyValue.of("workers", cnt)));
		return FStream.from(kvList)
						.map(kv -> String.format("%s=%s", kv.key(), kv.value()))
						.join(';');
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
