package marmot.optor;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;

import marmot.proto.optor.ValueAggregateProto;
import marmot.support.PBSerializable;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class AggregateFunction implements PBSerializable<ValueAggregateProto> {
	public AggrType m_type;
	public String m_aggrColumn;
	public String m_resultColumn;
	public List<String> m_args = Lists.newArrayList();
	
	public static AggregateFunction COUNT() {
		return new AggregateFunction(AggrType.COUNT, "", "count");
	}
	
	public static AggregateFunction MAX(String col) {
		return new AggregateFunction(AggrType.MAX, col, "max");
	}
	
	public static AggregateFunction MIN(String col) {
		return new AggregateFunction(AggrType.MIN, col, "min");
	}
	
	public static AggregateFunction SUM(String col) {
		return new AggregateFunction(AggrType.SUM, col, "sum");
	}
	
	public static AggregateFunction AVG(String col) {
		return new AggregateFunction(AggrType.AVG, col, "avg");
	}
	
	public static AggregateFunction STDDEV(String col) {
		return new AggregateFunction(AggrType.STDDEV, col, "stddev");
	}
	
	public static AggregateFunction CONVEX_HULL(String col) {
		return new AggregateFunction(AggrType.CONVEX_HULL, col, "the_geom");
	}
	
	public static AggregateFunction ENVELOPE(String col) {
		return new AggregateFunction(AggrType.ENVELOPE, col, "mbr");
	}
	
	public static AggregateFunction UNION(String col) {
		return new AggregateFunction(AggrType.GEOM_UNION, col, "union");
	}
	
	public static AggregateFunction CONCAT_STRING(String col, String delim) {
		return new AggregateFunction(AggrType.CONCAT_STR, col, "concat", delim);
	}
	
	public static AggregateFunction fromString(String expr) {
		String[] parts = expr.split(DELIM);
		AggrType type = AggrType.valueOf(parts[0]);
		AggregateFunction func = new AggregateFunction(type, parts[1], parts[2]);
		func.m_args = Arrays.asList(Arrays.copyOfRange(parts, 3, parts.length));
		
		return func;
	}
	
	public AggregateFunction(AggrType type, String aggrCol, String outCol) {
		m_type = type;
		m_aggrColumn = aggrCol;
		m_resultColumn = outCol;
	}
	
	public AggregateFunction(AggrType type, String aggrCol, String outCol,
							String... args) {
		m_type = type;
		m_aggrColumn = aggrCol;
		m_resultColumn = outCol;
		m_args = Arrays.asList(args);
	}
	
	public AggregateFunction as(String outCol) {
		m_resultColumn = outCol;
		return this;
	}
	
	private static final String DELIM = ",";
	public String toString() {
		String str = String.format("%s(%s)%s", m_type.name(), m_aggrColumn, m_resultColumn);
		if ( m_args != null && m_args.size() > 0 ) {
			str = str + DELIM + m_args.stream().collect(Collectors.joining(DELIM));
		}
		return str;
	}
	
	public static AggregateFunction fromProto(ValueAggregateProto proto) {
		String inputColName = proto.getInputColumn();
		
		AggregateFunction func = null;
		switch ( proto.getType() ) {
			case COUNT:
				func = COUNT();
				break;
			case MAX:
				func = MAX(inputColName);
				break;
			case MIN:
				func = MIN(inputColName);
				break;
			case SUM:
				func = SUM(inputColName);
				break;
			case AVG:
				func = AVG(inputColName);
				break;
			case STDDEV:
				func = STDDEV(inputColName);
				break;
			case CONVEX_HULL:
				func = CONVEX_HULL(inputColName);
				break;
			case ENVELOPE:
				func = ENVELOPE(inputColName);
				break;
			case GEOM_UNION:
				func = UNION(inputColName);
				break;
			default:
				throw new RuntimeException("unkown ValueAggregateProto: proto=" + proto);
		}
		
		switch ( proto.getOptionalOutputColumnCase() ) {
			case OUTPUT_COLUMN:
				func.as(proto.getOutputColumn());
				break;
			default:
		}
		
		return func;
	}

	@Override
	public ValueAggregateProto toProto() {
		ValueAggregateProto.Builder builder
						= ValueAggregateProto.newBuilder()
											.setType(m_type.toProto())
											.setInputColumn(m_aggrColumn)
											.setOutputColumn(m_resultColumn)
											.addAllParameter(m_args);
		
		return builder.build();
	}
}
