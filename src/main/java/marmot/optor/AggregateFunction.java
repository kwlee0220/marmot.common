package marmot.optor;

import javax.annotation.Nullable;

import utils.CSV;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class AggregateFunction {
	public AggregateType m_type;
	@Nullable public String m_aggrColumn;		// nullable
	public String m_resultColumn;
	@Nullable public String m_args;				// nullable
	
	public static AggregateFunction COUNT() {
		return new AggregateFunction(AggregateType.COUNT, null, "count");
	}
	
	public static AggregateFunction MAX(String col) {
		return new AggregateFunction(AggregateType.MAX, col, "max");
	}
	
	public static AggregateFunction MIN(String col) {
		return new AggregateFunction(AggregateType.MIN, col, "min");
	}
	
	public static AggregateFunction SUM(String col) {
		return new AggregateFunction(AggregateType.SUM, col, "sum");
	}

	public static AggregateFunction AVG(String col) {
		return new AggregateFunction(AggregateType.AVG, col, "avg");
	}
	
	public static AggregateFunction STDDEV(String col) {
		return new AggregateFunction(AggregateType.STDDEV, col, "stddev");
	}
	
	public static AggregateFunction CONVEX_HULL(String col) {
		return new AggregateFunction(AggregateType.CONVEX_HULL, col, "the_geom");
	}
	
	public static AggregateFunction ENVELOPE(String col) {
		return new AggregateFunction(AggregateType.ENVELOPE, col, "mbr");
	}
	
	public static AggregateFunction UNION_GEOM(String col) {
		return new AggregateFunction(AggregateType.UNION_GEOM, col, "the_geom");
	}
	
	public static AggregateFunction CONCAT_STR(String col, String... args) {
		String delim = args[0];
		return new AggregateFunction(AggregateType.CONCAT_STR, col, "concat", delim);
	}
	
	public AggregateFunction(AggregateType type, String aggrCol, String outCol) {
		m_type = type;
		m_aggrColumn = aggrCol;
		m_resultColumn = outCol;
	}
	
	public AggregateFunction(AggregateType type, String aggrCol, String outCol,
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
		String str = String.format("%s(%s)%s", m_type.name(),
									m_aggrColumn != null ? m_aggrColumn : "",
									m_resultColumn);
		if ( m_args != null ) {
			str = str + DELIM + m_args;
		}
		return str;
	}

    public static AggregateFunction fromProto(String proto) {
    	String[] parts = CSV.parseCsvAsArray(proto, '?', '\\');
    	String args = (parts.length > 1) ? parts[1] : null;
    	
    	CSV parser = CSV.get().withDelimiter(':').withQuote('"').withEscape('\\');
    	String[] comps = parser.parse(parts[0]).toArray(String.class);
    	if ( comps.length != 3 ) {
    		throw new IllegalArgumentException("invalid AggregateFunction: str=" + proto);
    	}
    	String aggrCol = comps[1].trim();
    	if ( aggrCol.length() == 0 ) {
    		aggrCol = null;
    	}
    	String outCol = comps[2].trim();
    	if ( outCol.length() == 0 ) {
    		outCol = null;
    	}

        AggregateType type = AggregateType.valueOf(comps[0].toUpperCase());
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
            case UNION_GEOM:
                func = UNION_GEOM(aggrCol);
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
