package marmot.optor;

import utils.CSV;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class AggregateFunction {
	public AggrType m_type;
	public String m_aggrColumn;		// nullable
	public String m_resultColumn;
	public String m_args;			// nullable
	
	public static AggregateFunction COUNT() {
		return new AggregateFunction(AggrType.COUNT, null, "count");
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
	
	public static AggregateFunction GEOM_UNION(String col) {
		return new AggregateFunction(AggrType.GEOM_UNION, col, "union");
	}
	
	public static AggregateFunction CONCAT_STR(String col, String... args) {
		String delim = args[0];
		return new AggregateFunction(AggrType.CONCAT_STR, col, "concat", delim);
	}
	
	public AggregateFunction(AggrType type, String aggrCol, String outCol) {
		m_type = type;
		m_aggrColumn = aggrCol;
		m_resultColumn = outCol;
	}
	
	public AggregateFunction(AggrType type, String aggrCol, String outCol,
							String args) {
		m_type = type;
		m_aggrColumn = aggrCol;
		m_resultColumn = outCol;
		m_args = args;
	}
	
	public AggregateFunction as(String outCol) {
		m_resultColumn = outCol;
		return this;
	}
	
	private static final String DELIM = "?";
	public String toString() {
		String str = String.format("%s(%s)%s", m_type.name(), m_aggrColumn, m_resultColumn);
		if ( m_args != null ) {
			str = str + DELIM + m_args;
		}
		return str;
	}

    public static AggregateFunction fromProto(String proto) {
    	String[] parts = CSV.parseAsArray(proto, '?', '\\');
    	
    	String args = (parts.length > 1) ? parts[1] : null;
    	String[] comps = CSV.parseAsArray(parts[0], ',', '\\');
    	if ( comps.length == 3 ) {
    		throw new IllegalArgumentException("invalid AggregateFunction: str=" + proto);
    	}
    	String aggrCol = comps[1].trim();
    	if ( aggrCol.length() == 0 ) {
    		aggrCol = null;
    	}
    	String outCol = comps[2].trim();

        AggrType type = AggrType.valueOf(comps[0].toUpperCase());
        AggregateFunction func;
        switch ( type ) {
            case COUNT:
                func = COUNT();
                break;
            case MAX:
                func = MAX(aggrCol);
                break;
            case MIN:
                func = MIN(aggrCol);
                break;
            case SUM:
                func = SUM(aggrCol);
                break;
            case AVG:
                func = AVG(aggrCol);
                break;
            case STDDEV:
                func = STDDEV(aggrCol);
                break;
            case CONVEX_HULL:
                func = CONVEX_HULL(aggrCol);
                break;
            case ENVELOPE:
                func = ENVELOPE(aggrCol);
                break;
            case GEOM_UNION:
                func = GEOM_UNION(aggrCol);
                break;
            case CONCAT_STR:
            	if ( args == null ) {
                    throw new IllegalArgumentException(
                    					String.format("invalid CONCAT_STR: proto=%s", proto));
            	}
                func = CONCAT_STR(aggrCol, args);
                break;
            default:
                throw new AssertionError();
        }
        if ( outCol != null ) {
            func.as(outCol);
        }

        return func;
    }
	
	public String toProto() {
		String header = String.format("%s:%s:%s",
										m_type.name().toLowerCase(),
										(m_aggrColumn != null) ? m_aggrColumn : "",
										m_resultColumn);
		if ( m_args != null ) {
			return header + "?" + FStream.of(m_args).join(",");
		}
		else {
			return header;
		}
	}
}
