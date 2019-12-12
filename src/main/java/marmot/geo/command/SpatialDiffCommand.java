package marmot.geo.command;

import static marmot.optor.JoinOptions.FULL_OUTER_JOIN;
import static marmot.optor.StoreDataSetOptions.FORCE;

import java.util.UUID;

import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.command.UsageHelp;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import utils.func.CheckedConsumer;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SpatialDiffCommand implements CheckedConsumer<MarmotRuntime> {
	@Mixin private Params m_params;
	@Mixin private UsageHelp m_help;
	private final String m_sessionId = UUID.randomUUID().toString();
	private int m_tmpDsCount = 1;
	
	private static class Params {
		@Parameters(paramLabel="left_dataset", index="0", arity="1..1",
					description={"left dataset id"})
		private String m_leftDsId;

		@Parameters(paramLabel="right_dataset", index="1", arity="1..1",
					description={"right dataset id"})
		private String m_rightDsId;

		@Parameters(paramLabel="output_dataset", index="2", arity="1..1",
					description={"output dataset id"})
		private String m_outputDsId;

		@Option(names={"-k", "-key"}, paramLabel="key_cols", required=true,
				description={"key columns"})
		private String m_keyCols;
		
		@Option(names={"-e", "-epsilon"}, paramLabel="epsilon", required=false,
				description={"delta value sloppiness"})
		private double m_epsilon = 1;
	}

	@Override
	public void accept(MarmotRuntime marmot) throws Exception {
		DataSet summary1 = summarize(marmot, m_params.m_leftDsId);
		DataSet summary2 = summarize(marmot, m_params.m_rightDsId);
//		DataSet summary1 = marmot.getDataSet("tmp/spatial_diff/test/1");
//		DataSet summary2 = marmot.getDataSet("tmp/spatial_diff/test/2");
		diff(marmot, summary1, summary2);
		
		marmot.deleteDataSet(summary1.getId());
		marmot.deleteDataSet(summary2.getId());
	}
	
	private void diff(MarmotRuntime marmot, DataSet ds1, DataSet ds2) {
		String outCols = String.format("left.{%s}, right.%s as %s2, "
									+ "left.%s as geom1, "
									+ "left.area as area1,"
									+ "left.centroid as centroid1,"
									+ "right.%s as geom2,"
									+ "right.area as area2,"
									+ "right.centroid as centroid2,",
									m_params.m_keyCols, m_params.m_keyCols,
									m_params.m_keyCols,
									ds1.getGeometryColumn(),
									ds2.getGeometryColumn());
		String adjustExpr = String.format("if ( %s == null ) { %s = %s2; }", m_params.m_keyCols,
										m_params.m_keyCols, m_params.m_keyCols); 
		String diffExpr = "if ( area1 != null && area2 != null ) {"
						+ 	"size_delta = Math.abs(area2-area1); "
						+ 	"center_dist = ST_Distance(centroid1, centroid2); "
						+ "}";
		String cmpExpr = String.format("size_delta == null || size_delta > %f "
										+ "|| center_dist > %f",
										m_params.m_epsilon, m_params.m_epsilon, m_params.m_epsilon);
		String prjExpr = String.format("%s,geom1,geom2,size_delta,center_dist",
										m_params.m_keyCols);
		
		Plan plan;
		plan = marmot.planBuilder("spatial diff")
					.loadHashJoin(ds1.getId(), m_params.m_keyCols,
									ds2.getId(), m_params.m_keyCols,
									outCols, FULL_OUTER_JOIN)
					.update(adjustExpr)
					.expand("size_delta:double,center_dist:double", diffExpr)
					.filter(cmpExpr)
					.project(prjExpr)
					.store(m_params.m_outputDsId, FORCE)
					.build();
		marmot.execute(plan);
	}
	
	private DataSet summarize(MarmotRuntime marmot, String dsId) {
		DataSet ds = marmot.getDataSet(dsId);
		GeometryColumnInfo gcInfo = ds.getGeometryColumnInfo();
		
		String areaExpr = String.format("ST_Area(%s)", gcInfo.name());
		String centerExpr = String.format("ST_Centroid(%s)", gcInfo.name());
		String prjExpr = String.format("%s,the_geom,area,centroid", m_params.m_keyCols);
		String outId = generateTempDataSetId();

		Plan plan;
		plan = marmot.planBuilder("summarize spatial information")
					.load(dsId)
					.defineColumn("area:double", areaExpr)
					.defineColumn("centroid:point", centerExpr)
					.project(prjExpr)
					.store(outId, FORCE(gcInfo))
					.build();
		marmot.execute(plan);
		
		return marmot.getDataSet(outId);
	}
	
	private String generateTempDataSetId() {
		return String.format("tmp/spatial_diff/%s/%d", m_sessionId, m_tmpDsCount++);
//		return String.format("tmp/spatial_diff/test/%d",m_tmpDsCount++);
	}
}
