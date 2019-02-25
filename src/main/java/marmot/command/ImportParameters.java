package marmot.command;

import picocli.CommandLine.Option;
import utils.Utilities;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ImportParameters extends StoreDataSetParameters {
	@Option(names="-dataset", paramLabel="id", required=true,
			description={"id of the dataset to import onto"})
	private String m_dsId;

	/**
	 * Import 대상 데이터세트 식별자를 반환한다.
	 * 
	 * @return 데이터세트 식별자
	 */
	public String getDataSetId() {
		return m_dsId;
	}
	
	/**
	 * Import 대상 데이터 세트의 식별자를 설정한다.
	 * 
	 * @param id	데이터세트 식별자.
	 */
	public void setDataSetId(String id) {
		Utilities.checkNotNullArgument(id, "dataset id is null");
		
		m_dsId = id;
	}
}