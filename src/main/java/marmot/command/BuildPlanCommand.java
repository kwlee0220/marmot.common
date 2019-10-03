package marmot.command;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.List;
import java.util.Map;

import io.vavr.CheckedConsumer;
import marmot.GeometryColumnInfo;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.PlanBuilder;
import marmot.StoreDataSetOptions;
import marmot.plan.GeomOpOptions;
import marmot.plan.SpatialJoinOptions;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import utils.KeyValue;
import utils.UnitUtils;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class BuildPlanCommand implements CheckedConsumer<MarmotRuntime> {
	@Mixin private UsageHelp m_help;
	
	@Parameters(paramLabel="operator description", index="0.*", arity="0..*",
				description={"operator description for the new operator"})
	private List<String> m_opExpr;

	@Option(names={"-f"}, paramLabel="plan_file", description={"target plan file to build-up"})
	private String m_file;

	@Option(names={"-i"}, paramLabel="input_plan_file", description={"input plan file"})
	private String m_input;

	@Option(names={"-o"}, paramLabel="output_plan_file", description={"output plan file"})
	private String m_output;
	
	@Option(names="-create", description={"create an empty plan"})
	private boolean m_create;
	
	@Override
	public void accept(MarmotRuntime marmot) throws Exception {
		Plan plan = loadPlan(marmot);
		
		if ( m_opExpr != null ) {
			plan = evaluate(marmot, plan);
		}
		
		Writer writer;
		if ( m_output != null ) {
			writer = new FileWriter(m_output);
		}
		else if ( m_file != null ) {
			writer = new FileWriter(m_file);
		}
		else {
			writer = new PrintWriter(System.out);
		}
		
		try ( Writer w = writer ) {
			writer.write(plan.toJson());
			writer.write("\n");
		}
	}
	
	private Plan evaluate(MarmotRuntime marmot, Plan plan) throws IOException {
		if ( m_opExpr.size() == 0 ) {
			throw new IllegalArgumentException("no operator spec");
		}
		
		PlanBuilder builder = plan.toBuilder();

		String op = m_opExpr.get(0);
		List<String> args = m_opExpr.subList(1, m_opExpr.size());
		
		switch ( op.toLowerCase() ) {
			case "load":
				builder = builder.load(args.get(0));
				break;
			case "filter":
				builder = builder.filter(args.get(0));
				break;
			case "project":
				builder = builder.project(args.get(0));
				break;
			case "store":
				addStore(builder, args);
				break;
			case "buffer":
				addBuffer(builder, args);
				break;
			case "centroid":
				addCentroid(builder, args);
				break;
			case "spatial_join":
				addSpatialJoin(builder, args);
				break;
			case "arc_clip":
				builder = builder.arcClip(args.get(0), args.get(1));
				break;
			case "shard":
				builder = builder.shard(Integer.parseInt(args.get(0)));
				break;
				
		}
		
		return builder.build();
	}
	
	private Plan loadPlan(MarmotRuntime marmot) throws IOException {
		if ( m_create ) {
			return marmot.planBuilder("plan").build();
		}
		
		Reader reader = null;
		if ( m_input != null ) {
			reader = new FileReader(m_input);
		}
		else if ( m_file != null ) {
			reader = new FileReader(m_file);
		}
		else {
			reader = new InputStreamReader(System.in);
		}
		
		try ( Reader r = reader ) {
			return Plan.parseJson(r);
		}
	}
	
	private void addSpatialJoin(PlanBuilder builder, List<String> args) {
		String geomCol = args.remove(0);
		String paramDsId = args.remove(0);
		
		String type = "inner";
		SpatialJoinOptions opts = SpatialJoinOptions.EMPTY;
		for ( KeyValue<String,String> kv: toKeyValueList(args) ) {
			switch ( kv.key().toLowerCase() ) {
				case "output":
					opts = opts.outputColumns(kv.value());
					break;
				case "negated":
					opts = opts.negated(Boolean.parseBoolean(kv.value()));
					break;
				case "join_expr":
					opts = opts.joinExpr(kv.value());
					break;
				case "type":
					type = kv.value();
					break;
			}
		}
		
		switch ( type.toLowerCase() ) {
			case "inner":
				builder.spatialJoin(geomCol, paramDsId, opts);
				break;
			case "semi":
				builder.spatialSemiJoin(geomCol, paramDsId, opts);
				break;
			case "outer":
				builder.spatialOuterJoin(geomCol, paramDsId, opts);
				break;
		}
	}
	
	private void addBuffer(PlanBuilder builder, List<String> args) {
		switch ( args.size() ) {
			case 1:
				builder.buffer("the_geom", UnitUtils.parseLengthInMeter(args.get(0)));
				break;
			case 2:
				builder.buffer(args.get(0), UnitUtils.parseLengthInMeter(args.get(1)));
				break;
			default:
				throw new IllegalArgumentException("invalid buffer args: " + args);
		}
	}
	
	private void addStore(PlanBuilder builder, List<String> args) {
		String dsId = args.remove(0);
		
		StoreDataSetOptions opts = StoreDataSetOptions.EMPTY;
		for ( KeyValue<String,String> kv: toKeyValueList(args) ) {
			switch ( kv.key().toLowerCase() ) {
				case "geom_col":
					opts = opts.geometryColumnInfo(GeometryColumnInfo.fromString(kv.value()));
					break;
				case "force":
					opts = opts.force(Boolean.parseBoolean(kv.value()));
					break;
				case "compress":
					opts = opts.compressionCodecName(kv.value());
					break;
			}
		}
		builder.store(dsId, opts);
	}
	
	private void addCentroid(PlanBuilder builder, List<String> args) {
		String geomCol = args.remove(0);
		
		GeomOpOptions opts = GeomOpOptions.DEFAULT;
		for ( KeyValue<String,String> kv: toKeyValueList(args) ) {
			switch ( kv.key().toLowerCase() ) {
				case "output":
					opts.outputColumn(kv.value());
					break;
			}
		}
		
		builder.centroid(geomCol, false, opts);
	}
	
	private Map<String,String> toArgumentMap(List<String> args) {
		return FStream.from(args)
						.map(KeyValue::parse)
						.toMap(KeyValue::key, KeyValue::value);
	}
	
	private List<KeyValue<String,String>> toKeyValueList(List<String> args) {
		return FStream.from(args)
						.map(arg -> KeyValue.parse(arg, '\''))
						.toList();
	}
}
