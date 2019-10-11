package marmot.command.plan;

import com.vividsolutions.jts.geom.Envelope;

import marmot.MarmotRuntime;
import marmot.PlanBuilder;
import marmot.geo.GeoClientUtils;
import marmot.plan.PredicateOptions;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="query", description="add a 'query' operator")
class AddQuery extends AbstractAddOperatorCommand {
	@Parameters(paramLabel="dataset_id", index="0",
				description={"dataset id to load"})
	private String m_dsId;
	
	@Option(names={"-key_dataset"}, paramLabel="dataset_id",
			description="key dataset id")
	private String m_keyDsId;
	
	@Option(names={"-bounds"}, paramLabel="envelope_string",
			description="key bounds")
	private String m_boundsStr;
	
	private PredicateOptions m_opts = PredicateOptions.DEFAULT;
	
	@Option(names={"-negated"}, description="negated search")
	private void setNegated(boolean flag) {
		m_opts = m_opts.negated(flag);
	}

	@Override
	public PlanBuilder add(MarmotRuntime marmot, PlanBuilder builder) throws Exception {
		if ( m_boundsStr != null ) {
			Envelope bounds = GeoClientUtils.parseEnvelope(m_boundsStr).get();
			return builder.query(m_dsId, bounds, m_opts);
		}
		else if ( m_keyDsId != null ) {
			return builder.query(m_dsId, m_keyDsId, m_opts);
		}
		
		throw new IllegalArgumentException("query key is not defined");
	}
}
