package marmot.plan;

import marmot.proto.optor.LoadJdbcTableOptionsProto;
import marmot.support.PBSerializable;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class LoadJdbcTableOptions implements PBSerializable<LoadJdbcTableOptionsProto> {
	private FOption<String> m_selectExpr = FOption.empty();
	private FOption<Integer> m_mapperCount = FOption.empty();
	
	public static LoadJdbcTableOptions create() {
		return new LoadJdbcTableOptions();
	}
	
	public FOption<String> selectExpr() {
		return m_selectExpr;
	}
	
	public LoadJdbcTableOptions selectExpr(String outCol) {
		m_selectExpr = FOption.ofNullable(outCol);
		return this;
	}
	
	public FOption<Integer> mapperCount() {
		return m_mapperCount;
	}
	
	public LoadJdbcTableOptions mapperCount(int count) {
		m_mapperCount = FOption.of(count);
		return this;
	}

	public static LoadJdbcTableOptions fromProto(LoadJdbcTableOptionsProto proto) {
		LoadJdbcTableOptions opts = LoadJdbcTableOptions.create();
		
		switch ( proto.getOptionalSelectExprCase() ) {
			case SELECT_EXPR:
				opts.selectExpr(proto.getSelectExpr());
				break;
			default:
		}
		switch ( proto.getOptionalMapperCountCase() ) {
			case MAPPER_COUNT:
				opts.mapperCount(proto.getMapperCount());
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
