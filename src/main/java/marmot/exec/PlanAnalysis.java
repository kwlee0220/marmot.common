package marmot.exec;

import marmot.ExecutePlanOptions;
import marmot.Plan;
import marmot.proto.service.MarmotAnalysisProto;
import marmot.proto.service.MarmotAnalysisProto.MemberCase;
import marmot.proto.service.MarmotAnalysisProto.PlanExecProto;
import utils.Utilities;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PlanAnalysis extends MarmotAnalysis {
	private final Plan m_plan;
	private final ExecutePlanOptions m_opts;
	
	public PlanAnalysis(String id, Plan plan, ExecutePlanOptions opts) {
		super(id, Type.PLAN);
		
		m_plan = plan;
		m_opts = opts;
	}
	
	public PlanAnalysis(String id, Plan plan) {
		this(id, plan, ExecutePlanOptions.DEFAULT);
	}

	public Plan getPlan() {
		return m_plan;
	}

	public ExecutePlanOptions getExecuteOptions() {
		return m_opts;
	}
	
	@Override
	public String toString() {
		return String.format("%s[%s]: %n%s", getType(), getId(), m_plan);
	}
	
	public static PlanAnalysis fromProto(MarmotAnalysisProto proto) {
		Utilities.checkArgument(proto.getMemberCase().equals(MemberCase.PLAN_EXEC),
								"not PlanAnalysis");
		
		PlanExecProto planExec = proto.getPlanExec();
		Plan plan = Plan.fromProto(planExec.getPlan());
		ExecutePlanOptions opts = ExecutePlanOptions.fromProto(planExec.getOptions());
		return new PlanAnalysis(proto.getId(), plan, opts);
	}

	@Override
	public MarmotAnalysisProto toProto() {
		PlanExecProto execProto = PlanExecProto.newBuilder()
												.setPlan(m_plan.toProto())
												.setOptions(m_opts.toProto())
												.build();
		return MarmotAnalysisProto.newBuilder()
									.setId(getId())
									.setPlanExec(execProto)
									.build();
	}
}
