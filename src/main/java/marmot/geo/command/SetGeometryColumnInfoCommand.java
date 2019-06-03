package marmot.geo.command;

import io.vavr.CheckedConsumer;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.MarmotRuntime;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Parameters;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SetGeometryColumnInfoCommand implements CheckedConsumer<MarmotRuntime> {
	@Mixin private Params m_params;
	
	public static class Params {
		@Parameters(paramLabel="id", index="0", arity="1..1",
					description={"dataset id to set its geometry column info"})
		private String m_dataset;

		private GeometryColumnInfo m_gcInfo;
		@Parameters(paramLabel="column_name(EPSG code)", index="1", arity="0..1",
				description={"new geometry column info"})
		public void setGeometryColumnInfo(String gcInfoStr) {
			m_gcInfo = GeometryColumnInfo.fromString(gcInfoStr);
		}
	}

	@Override
	public void accept(MarmotRuntime marmot) throws Exception {
		DataSet ds = marmot.getDataSet(m_params.m_dataset);
		ds.updateGeometryColumnInfo(FOption.ofNullable(m_params.m_gcInfo));
	}
}
