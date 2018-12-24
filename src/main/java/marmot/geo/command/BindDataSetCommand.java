package marmot.geo.command;

import io.vavr.CheckedConsumer;
import marmot.DataSet;
import marmot.DataSetType;
import marmot.GeometryColumnInfo;
import marmot.MarmotRuntime;
import marmot.command.UsageHelp;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class BindDataSetCommand implements CheckedConsumer<MarmotRuntime> {
	@Mixin private Params m_params;
	@Mixin private UsageHelp m_help;
	
	public static class Params {
		@Parameters(paramLabel="path", index="0", arity="1..1",
					description={"source file-path (or source dataset-id) to bind"})
		private String m_path;
		
		@Option(names={"-t", "-type"}, paramLabel="type", required=true,
				description={"dataset type ('csv','custom','file', or 'dataset)"})
		private String m_type;
		
		@Option(names={"-d", "-dataset"}, paramLabel="dataset_id", required=true,
				description="output dataset name")
		private String m_dataset;

		private GeometryColumnInfo m_gcInfo;
		@Option(names={"-geom_col"}, paramLabel="column_name(EPSG code)",
				description="default Geometry column info")
		public void setGeometryColumnInfo(String gcInfoStr) {
			m_gcInfo = GeometryColumnInfo.fromString(gcInfoStr);
		}
	}

	@Override
	public void accept(MarmotRuntime marmot) throws Exception {
		DataSetType type;
		if ( "dataset".equals(m_params.m_type) ) {
			DataSet srcDs = marmot.getDataSet(m_params.m_path);
			m_params.m_path = srcDs.getHdfsPath();
			type = srcDs.getType();
			
			if ( srcDs.hasGeometryColumn() ) {
				m_params.m_gcInfo = srcDs.getGeometryColumnInfo();
			}
		}
		else {
			type = DataSetType.fromString(m_params.m_type.toUpperCase());
		}

		marmot.deleteDataSet(m_params.m_dataset);
		if ( m_params.m_gcInfo != null ) {
			marmot.bindExternalDataSet(m_params.m_dataset, m_params.m_path, type,
										m_params.m_gcInfo);
		}
		else {
			marmot.bindExternalDataSet(m_params.m_dataset, m_params.m_path, type);
		}
	}
}
