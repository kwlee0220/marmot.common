package marmot.command;

import java.io.File;
import java.io.InputStream;
import java.nio.charset.Charset;

import io.vavr.CheckedConsumer;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.plan.STScriptPlanLoader;
import marmot.proto.optor.OperatorProto;
import marmot.proto.optor.StoreIntoDataSetProto;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Parameters;
import utils.func.FOption;
import utils.io.IOUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RunPlanCommand implements CheckedConsumer<MarmotRuntime> {
	@Mixin private StoreDataSetParameters m_storeParams;
	@Mixin private UsageHelp m_help;

	@Parameters(paramLabel="plan_path", index="0", arity="1..1",
				description="plan file path to run")
	private String m_planPath;
	@Parameters(paramLabel="output", index="1", arity="1..1",
				description="dataset id for the result")
	private String m_outDsId;
	
	@Override
	public void accept(MarmotRuntime marmot) throws Exception {
		String script;
		if ( m_planPath != null ) {
			script = IOUtils.toString(new File(m_planPath));
		}
		else {
			try ( InputStream is = System.in ) {
				script = IOUtils.toString(is, Charset.defaultCharset());
			}
		}
		Plan plan  = STScriptPlanLoader.load(script);
	
		if ( !m_storeParams.getAppend() ) {
			String fromPlanDsId = getStoreTargetDataSetId(plan).getOrNull();
			if ( m_outDsId == null && fromPlanDsId == null ) {
				throw new IllegalArgumentException("result dataset id is messing");
			}
			else if ( m_outDsId == null ) {
				m_outDsId = fromPlanDsId;
			}

			marmot.createDataSet(m_outDsId, plan, m_storeParams.toOptions());
		}
		else {
			plan = adjustPlanForStore(m_outDsId, plan);
			marmot.execute(plan);
		}
	}
	
	private static FOption<String> getStoreTargetDataSetId(Plan plan) {
		OperatorProto last = plan.getLastOperator()
								.getOrElseThrow(() -> new IllegalArgumentException("plan is empty"));
		switch ( last.getOperatorCase() ) {
			case STORE_INTO_DATASET:
				return FOption.of(last.getStoreIntoDataset().getId());
			default:
				return FOption.empty();
		}
	}
	
	private static Plan adjustPlanForStore(String dsId, Plan plan) {
		OperatorProto last = plan.getLastOperator()
								.getOrElseThrow(() -> new IllegalArgumentException("plan is empty"));
		switch ( last.getOperatorCase() ) {
			case STORE_INTO_DATASET:
			case STORE_AS_CSV:
			case STORE_INTO_JDBC_TABLE:
			case STORE_AND_RELOAD:
			case STORE_AS_HEAPFILE:
				return plan;
			default:
				StoreIntoDataSetProto store = StoreIntoDataSetProto.newBuilder()
																	.setId(dsId)
																	.build();
				OperatorProto op = OperatorProto.newBuilder().setStoreIntoDataset(store).build();
				return Plan.fromProto(plan.toProto()
											.toBuilder()
											.addOperators(op)
											.build());
		}
	}
}
