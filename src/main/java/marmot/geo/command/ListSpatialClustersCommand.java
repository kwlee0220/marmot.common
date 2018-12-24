package marmot.geo.command;

import io.vavr.CheckedConsumer;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.Record;
import marmot.RecordSet;
import marmot.command.UsageHelp;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Parameters;
import utils.UnitUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ListSpatialClustersCommand implements CheckedConsumer<MarmotRuntime> {
	@Parameters(paramLabel="dataset-id", index="0", arity="1..1",
				description={"id of the target dataset"})
	private String m_dsId;
	@Mixin private UsageHelp m_help;
	
	@Override
	public void accept(MarmotRuntime marmot) throws Exception {
		Plan plan;
		plan = marmot.planBuilder("list_spatial_clusters")
					.loadSpatialClusterIndexFile(m_dsId)
					.project("*-{bounds,value_envelope}")
					.build();
		try ( RecordSet rset = marmot.executeLocally(plan) ) {
			rset.forEach(r -> printIndexEntry(r));
		}
	}
	
	private static final void printIndexEntry(Record record) {
		String packId = record.getString("pack_id");
		int blockNo = record.getInt("block_no");
		String quadKey = record.getString("quad_key");
		long count = record.getLong("count");
		String start = UnitUtils.toByteSizeString(record.getLong("start"));
		String len = UnitUtils.toByteSizeString(record.getLong("length"));
		
		System.out.printf("pack_id=%s, block_no=%02d, quad_key=%s, count=%d, start=%s, length=%s%n",
							packId, blockNo, quadKey, count, start, len);
	}
}
