package marmot.plan;

import java.util.List;

import org.apache.commons.lang3.ArrayUtils;

import com.google.common.base.Preconditions;

import marmot.proto.optor.LoadJdbcTableProto.OptionsProto;
import utils.Utilities;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class LoadJdbcTableOption {
	public abstract void set(OptionsProto.Builder builder);
	
	public static MapperCountOption MAPPER_COUNT(int count) {
		return new MapperCountOption(count);
	}
	
	public static SelectOption SELECT(String expr) {
		Utilities.checkNotNullArgument(expr, "column_expression is null");
	
		return new SelectOption(expr);
	}
	
	public static OptionsProto toProto(List<LoadJdbcTableOption> opts) {
		Utilities.checkNotNullArgument(opts, "LoadJdbcTableOptions are null");
		Preconditions.checkArgument(opts.size() > 0, "LoadJdbcTableOption is empty"); 
		
		return FStream.from(opts)
					.collectLeft(OptionsProto.newBuilder(),
								(b,o) -> o.set(b))
					.build();
	}

	public static LoadJdbcTableOption[] fromProto(OptionsProto proto) {
		LoadJdbcTableOption[] opts = new LoadJdbcTableOption[0];

		switch ( proto.getOptionalColumnsExprCase() ) {
			case COLUMNS_EXPR:
				opts = ArrayUtils.add(opts, SELECT(proto.getColumnsExpr()));
				break;
			default:
		}
		switch ( proto.getOptionalMapperCountCase() ) {
			case MAPPER_COUNT:
				opts = ArrayUtils.add(opts, MAPPER_COUNT(proto.getMapperCount()));
				break;
			default:
		}
		
		return opts;
	}
	
	public static class SelectOption extends LoadJdbcTableOption {
		private final String m_columnExpr;
		
		private SelectOption(String header) {
			m_columnExpr = header;
		}
		
		public String get() {
			return m_columnExpr;
		}
		
		public void set(OptionsProto.Builder builder) {
			builder.setColumnsExpr(m_columnExpr);
		}
		
		@Override
		public String toString() {
			return String.format("select=%s", m_columnExpr);
		}
	}
	
	public static class MapperCountOption extends LoadJdbcTableOption {
		private final int m_count;
		
		private MapperCountOption(int count) {
			m_count = count;
		}
		
		public int get() {
			return m_count;
		}
		
		public void set(OptionsProto.Builder builder) {
			builder.setMapperCount(m_count);
		}
		
		@Override
		public String toString() {
			return String.format("mapper_count=%s", m_count);
		}
	}
}
