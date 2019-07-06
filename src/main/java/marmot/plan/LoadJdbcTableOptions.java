package marmot.plan;

import marmot.proto.optor.LoadJdbcTableOptionsProto;
import marmot.support.PBSerializable;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class LoadJdbcTableOptions implements PBSerializable<LoadJdbcTableOptionsProto> {
	private final FOption<String> m_selectExpr;
	private final FOption<Integer> m_mapperCount;
	
	private LoadJdbcTableOptions(FOption<String> selectExpr, FOption<Integer> mapperCount) {
		m_selectExpr = selectExpr;
		m_mapperCount = mapperCount;
	}
	
	public static LoadJdbcTableOptions DEFAULT() {
		return new LoadJdbcTableOptions(FOption.empty(), FOption.empty());
	}
	
	public static LoadJdbcTableOptions SELECT(String expr) {
		return new LoadJdbcTableOptions(FOption.of(expr), FOption.empty());
	}
	
	public FOption<String> selectExpr() {
		return m_selectExpr;
	}
	
	public LoadJdbcTableOptions selectExpr(String outCol) {
		return new LoadJdbcTableOptions(FOption.ofNullable(outCol), m_mapperCount);
	}
	
	public FOption<Integer> mapperCount() {
		return m_mapperCount;
	}
	
	public LoadJdbcTableOptions mapperCount(int count) {
		return new LoadJdbcTableOptions(m_selectExpr, FOption.of(count));
	}

	public static LoadJdbcTableOptions fromProto(LoadJdbcTableOptionsProto proto) {
		LoadJdbcTableOptions opts = LoadJdbcTableOptions.DEFAULT();
		
		switch ( proto.getOptionalSelectExprCase() ) {
			case SELECT_EXPR:
				opts = opts.selectExpr(proto.getSelectExpr());
				break;
			default:
		}
		switch ( proto.getOptionalMapperCountCase() ) {
			case MAPPER_COUNT:
				opts = opts.mapperCount(proto.getMapperCount());
				break;
			default:
		}
		
		return opts;
	}

	@Override
	public LoadJdbcTableOptionsProto toProto() {
		LoadJdbcTableOptionsProto.Builder builder = LoadJdbcTableOptionsProto.newBuilder();
		
		m_selectExpr.ifPresent(builder::setSelectExpr);
		m_mapperCount.ifPresent(builder::setMapperCount);
		
		return builder.build();
	}
}
