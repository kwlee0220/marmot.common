package marmot.command;

import picocli.CommandLine.Option;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ImportParameters extends StoreDataSetParameters {
	@Option(names="-dataset", paramLabel="id", required=true,
			description={"id of the dataset to import onto"})
	private String m_dsId;
	
	public String getDataSetId() {
		return m_dsId;
	}
	
	public void setDataSetId(String id) {
		m_dsId = id;
	}
}