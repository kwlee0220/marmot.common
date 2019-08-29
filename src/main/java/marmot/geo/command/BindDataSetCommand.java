package marmot.geo.command;

import io.vavr.CheckedConsumer;
import marmot.BindDataSetOptions;
import marmot.DataSet;
import marmot.DataSetType;
import marmot.GeometryColumnInfo;
import marmot.MarmotRuntime;
import marmot.command.UsageHelp;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import utils.Utilities;

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
		
		@Parameters(paramLabel="dataset_id", index="1", arity="1..1",
				description={"dataset id to bind into"})
		private String m_dataset;
		
		@Option(names={"-t", "-type"}, paramLabel="type", required=true,
				description={"source type ('text', 'file', or 'dataset)"})
		private String m_type;

		private GeometryColumnInfo m_gcInfo;
		@Option(names={"-geom_col"}, paramLabel="column_name(EPSG code)",
				description="default Geometry column info")
		public void setGeometryColumnInfo(String gcInfoStr) {
			m_gcInfo = GeometryColumnInfo.fromString(gcInfoStr);
		}
		
		@Option(names={"-f", "-force"}, description="force to bind to a new dataset")
		private boolean m_force;
	}

	@Override
	public void accept(MarmotRuntime marmot) throws Exception {
		DataSetType type;
		switch ( m_params.m_type ) {
			case "text":
				type = DataSetType.TEXT;
				break;
			case "file":
				type = DataSetType.FILE;
				break;
			case "dataset":
				DataSet srcDs = marmot.getDataSet(m_params.m_path);
				if ( m_params.m_gcInfo == null && srcDs.hasGeometryColumn() ) {
					m_params.m_gcInfo = srcDs.getGeometryColumnInfo();
				}
				m_params.m_path = srcDs.getHdfsPath();
				type = DataSetType.LINK;
				break;
			default:
				throw new IllegalArgumentException("invalid dataset type: " + m_params.m_type);
		}
		
		BindDataSetOptions opts = BindDataSetOptions.FORCE(m_params.m_force);
		if ( m_params.m_gcInfo != null ) {
			opts = opts.geometryColumnInfo(m_params.m_gcInfo);
		}
		marmot.bindExternalDataSet(m_params.m_dataset, m_params.m_path, type, opts);
	}
}
