package marmot.plan;

import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import utils.CSV;
import utils.KeyValue;
import utils.Utilities;
import utils.func.FOption;
import utils.func.Funcs;
import utils.stream.FStream;

import marmot.RecordSchema;
import marmot.proto.optor.GroupByKeyProto;
import marmot.support.PBSerializable;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Group implements Serializable, PBSerializable<GroupByKeyProto> {
	private static final long serialVersionUID = 1L;
	
	private final String m_keys;
	private FOption<String> m_tagCols = FOption.empty();
	private FOption<String> m_orderBy = FOption.empty();
	private FOption<String> m_partCol = FOption.empty();
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
	
	public FOption<String> partitionColumn() {
		return m_partCol;
	}
	
	public Group partitionColumn(String colIdx) {
		m_partCol = FOption.of(colIdx);
		return this;
	}
	
	public FOption<Integer> workerCount() {
		return m_workerCount;
	}
	
	public Group workerCount(int workerCount) {
		m_workerCount = FOption.of(workerCount);
		return this;
	}
	
	public RecordSchema toKeySchema(RecordSchema inputSchema) {
		return inputSchema.project(CSV.parseCsv(m_keys).toList());
	}
	
	public RecordSchema toTagSchema(RecordSchema inputSchema) {
		return m_tagCols.map(csv -> inputSchema.project(CSV.parseCsv(csv).toList()))
						.getOrElse(RecordSchema.NULL);
	}
	
	public RecordSchema toOrderSchema(RecordSchema inputSchema) {
		return m_orderBy.map(csv -> inputSchema.project(CSV.parseCsv(csv).toList()))
						.getOrElse(RecordSchema.NULL);
	}
	
	public RecordSchema toFullKeySchema(RecordSchema inputSchema) {
		Set<String> fullKeys = Sets.newLinkedHashSet();
		CSV.parseCsv(m_keys).toCollection(fullKeys);
		m_tagCols.ifPresent(csv -> CSV.parseCsv(csv).toCollection(fullKeys));
		m_orderBy.ifPresent(csv -> CSV.parseCsv(csv).toCollection(fullKeys));
		m_partCol.ifPresent(csv -> CSV.parseCsv(csv).toCollection(fullKeys));
		
		return inputSchema.project(fullKeys);
	}
	
	public RecordSchema toValueSchema(RecordSchema inputSchema) {
		Set<String> fullKeys = Sets.newHashSet();
		CSV.parseCsv(m_keys).toCollection(fullKeys);
		m_tagCols.ifPresent(csv -> CSV.parseCsv(csv).toCollection(fullKeys));
		m_orderBy.ifPresent(csv -> CSV.parseCsv(csv).toCollection(fullKeys));
		m_partCol.ifPresent(csv -> CSV.parseCsv(csv).toCollection(fullKeys));
		
		return inputSchema.complement(fullKeys);
	}
	
	public static Group parseGroup(String expr) {
		Utilities.checkNotNullArgument(expr, "Group string rep is null");
	
		Map<String,String> kvMap = CSV.parseCsv(expr, ';')
									.map(Group::parseKeyValue)
									.toKeyValueStream(KeyValue::key, KeyValue::value)
									.toMap();
		String keyCols = kvMap.get("keys");
		if ( keyCols == null ) {
			throw new IllegalArgumentException("'keys' is not present: expr=" + expr);
		}
		
		Group group = Group.ofKeys(keyCols);
		Funcs.acceptIfPresent(kvMap, "tags", (k,v) -> group.tags(v));
		Funcs.acceptIfPresent(kvMap, "orderBy", (k,v) -> group.orderBy(v));
		Funcs.acceptIfPresent(kvMap, "partColumn", (k,v) -> group.partitionColumn(v));
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
		m_partCol.ifPresent(col -> kvList.add(KeyValue.of("partColumn", col)));
		m_workerCount.map(cnt -> "" + cnt)
					.ifPresent(cnt -> kvList.add(KeyValue.of("workers", cnt)));
		return FStream.from(kvList)
						.map(kv -> String.format("%s=%s", kv.key(), kv.value()))
						.join(';');
	}
	
	private Object writeReplace() {
		return new SerializationProxy(this);
	}
	
	private void readObject(ObjectInputStream stream) throws InvalidObjectException {
		throw new InvalidObjectException("Use Serialization Proxy instead.");
	}

	private static class SerializationProxy implements Serializable {
		private static final long serialVersionUID = 1L;
		
		private final GroupByKeyProto m_proto;
		
		private SerializationProxy(Group group) {
			m_proto = group.toProto();
		}
		
		private Object readResolve() {
			return Group.fromProto(m_proto);
		}
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
		switch ( proto.getOptionalPartitionColumnCase() ) {
			case PARTITION_COLUMN:
				opts.partitionColumn(proto.getPartitionColumn());
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
		m_partCol.ifPresent(builder::setPartitionColumn);
		m_workerCount.ifPresent(builder::setGroupWorkerCount);
		
		return builder.build();
	}
}
