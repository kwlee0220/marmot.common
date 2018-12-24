package marmot.command;

import java.io.File;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.List;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import io.vavr.CheckedConsumer;
import marmot.DataSetOption;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.plan.STScriptPlanLoader;
import marmot.proto.optor.OperatorProto;
import marmot.proto.optor.StoreIntoDataSetProto;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
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

	@Parameters(paramLabel="output", index="0", arity="0..1",
				description="dataset id for the result")
	private String m_outDsId;
	@Option(names={"-plan"}, paramLabel="file", description="plan file path to run")
	private String m_planPath = null;
	
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
		String planJson = STScriptPlanLoader.toJson(script);
		
		Plan plan = Plan.parseJson(planJson);
	
		if ( !m_storeParams.getAppend() ) {
			List<DataSetOption> optList = Lists.newArrayList();
			
			m_storeParams.getGeometryColumnInfo()
						.ifPresent(gcInfo -> optList.add(DataSetOption.GEOMETRY(gcInfo)));
			
			if ( m_storeParams.getForce() ) {
				optList.add(DataSetOption.FORCE);
			}
			
			m_storeParams.getBlockSize()
						.ifPresent(blkSz -> optList.add(DataSetOption.BLOCK_SIZE(blkSz)));
			m_storeParams.getCompress()
						.filter(f -> f)
						.ifPresent(f -> optList.add(DataSetOption.COMPRESS));
			
			String fromPlanDsId = getStoreTargetDataSetId(plan).getOrNull();
			if ( m_outDsId == null && fromPlanDsId == null ) {
				throw new IllegalArgumentException("result dataset id is messing");
			}
			else if ( m_outDsId == null ) {
				m_outDsId = fromPlanDsId;
			}

			marmot.createDataSet(m_outDsId, plan, Iterables.toArray(optList, DataSetOption.class));
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
