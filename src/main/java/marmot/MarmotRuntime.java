package marmot;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import com.vividsolutions.jts.geom.Geometry;

import marmot.exec.AnalysisNotFoundException;
import marmot.exec.CompositeAnalysis;
import marmot.exec.ExecutionNotFoundException;
import marmot.exec.MarmotAnalysis;
import marmot.exec.MarmotExecution;
import marmot.exec.MarmotExecutionException;
import marmot.exec.PlanAnalysis;
import marmot.io.MarmotFileNotFoundException;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface MarmotRuntime {
	/**
	 * 주어진 식별자에 해당하는 데이터세트({@link DataSet}) 객체를 반환한다.
	 * 
	 * @param id	데이터세트 식별자
	 * @return	DataSet 객체
	 * @throws DataSetNotFoundException	식별자에 해당하는 데이터세트가 없는 경우.
	 */
	public DataSet getDataSet(String id);
	
	/**
	 * 주어진 식별자에 해당하는 데이터세트({@link DataSet}) 객체를 반환한다.
	 * 식별자에 해당하는 데이터세트가 없는 경우는 {@code null}을 반환한다.
	 * 
	 * @param id	데이터세트 식별자
	 * @return	DataSet 객체
	 */
	public DataSet getDataSetOrNull(String id);
	
	/**
	 * 시스템에 등록된 모든 데이터세트를 반환한다.
	 * 
	 * @return	데이터세트 리스트.
	 */
	public List<DataSet> getDataSetAll();
	
	/**
	 * 주어진 식별자에 해당하는 데이터세트를 삭제시킨다.
	 * 
	 * @param id	대상 데이터세트 식별자.
	 * @return	 데이터세트 삭제 여부.
	 */
	public boolean deleteDataSet(String id);
	
	/**
	 * 데이터세트의 이름을 변경시킨다.
	 * 
	 * @param id 	변경시킬 데이터세트 식별자.
	 * @param newId 변경될 데이터세트 식별자.
	 */
	public void moveDataSet(String id, String newId);
	
	/**
	 * 주어진 이름의 폴더에 저장된 모든 데이터세트를 반환한다.
	 * <p>
	 * 폴더는 계층구조를 갖고 있기 때문에 {@code recursive} 인자에 따라
	 * 지정된 폴더에 저장된 데이터세트를 반환할 수도 있고, 해당 폴더와
	 * 모든 하위 폴더에 저장된 데이터세트들을 반환하게 할 수 있다. 
	 * 
	 * @param folder	대상 폴더 이름.
	 * @param recursive	하위 폴더 포함 여부.
	 * @return	데이터세트 설정정보 리스트.
	 */
	public List<DataSet> getDataSetAllInDir(String folder, boolean recursive);

	/**
	 * 데이터세트를 생성한다. 생성된 데이터세트의 정보는 Marmot 카타로그에 등록된다.
	 * <p>
	 * 데이터세트 생성시 추가의 옵션을 통해 추가의 정보ㅓ를 제공할 수 있다. 이때 사용할 수 있는
	 * 옵션은 다음과 같다.
	 * <dl>
	 * 	<dt>DataSetOption.GEOMETRY(col,srid)</dt>
	 * 	<dd>생성할 데이터세트에서 사용할 기본 공간 컬럼 정보</dd>
	 * 	<dt>DataSetOption.FORCE</dt>
	 * 	<dd>생성하고자 하는 식별자의 데이터세트가 이미 존재하는 경우, 생성 전에 해당 데이터세트를 삭제함.</dd>
	 * 	<dt>DataSetOption.BLOCK_SIZE(n)</dt>
	 * 	<dd>생성할 데이터세트의 블럭 크기</dd>
	 * 	<dt>DataSetOption.COMPRESS</dt>
	 * 	<dd>생성할 데이터세트에 저장될 내용의 압축 사용</dd>
	 * </dl>
	 * 예를들어 다음의 예는 'the_geom'을 기본 공간  컬럼으로 데이터세트를 생성하는 예를 보여준다.<pre>
	 * <code>
	 * MarmotRuntime marmot = ...;
	 * RecordSchema schema = RecordSchema.builder()
	 * 									.addColumn("the_geom", DataSet.POINT)
	 * 									......
	 * 									.build();
	 * marmot.createDataSet("test", schema, DataSetOption.GEOMETRY("the_geom", "EPSG:4326"),
	 * 										DataSetOption.FORCE);
	 * </code></pre>
	 * 
	 * @param dsId		생성될 데이터세트의 식별자.
	 * @param schema	생성될 데이터세트의 스키마 정보.
	 * @param opts		데이터세트 생성에 필요한 옵션 리스트.
	 * @return	 생성된 데이터세트 객체.
	 * @throws DataSetExistsException	동일 경로명의 데이터세트가 이미 존재하는 경우.
	 */
	public DataSet createDataSet(String dsId, RecordSchema schema, CreateDataSetOptions opts)
		throws DataSetExistsException;
	
	/**
	 * 기존 데이터세트와 바인딩시킨다.
	 * 
	 * @param dsId	바인딩된 데이터세트 식별자.
	 * @param srcPath	바인딩할 원시 데이터의 식별자.
	 * @param type	원시 데이터 형태.
	 * @param opts	바인딩된 데이터세트에 설정할 옵션 정보.
	 * @return	바인딩되어 생성된 데이터 세트 객체.
	 */
	public DataSet bindExternalDataSet(String dsId, String srcPath, DataSetType type,
										BindDataSetOptions opts);
	
	/**
	 * 시스템에 등록된 모든 폴더의 이름들을 반환한다.
	 * 
	 * @return	폴더 이름 리스트.
	 */
	public List<String> getDirAll();
	
	/**
	 * 주어진 이름의 폴더에 등록된 모든 하위 폴더 이름을 반환한다.
	 * <p>
	 * 폴더는 계층구조를 갖고 있기 때문에 {@code recursive} 인자에 따라
	 * 지정된 폴더에 바로 속한 하위 폴더의 이름들만 반환할 수도 있고, 해당 폴더의
	 * 모든 하위 폴더의 이름을 반환하게 할 수 있다. 
	 * 
	 * @param folder	대상 폴더 이름.
	 * @param recursive	하위 폴더 포함 여부.
	 * @return	폴더 이름 리스트.
	 */
	public List<String> getSubDirAll(String folder, boolean recursive);
	
	/**
	 * 주어진 이름의 폴더의 상위 폴더 이름을 반환한다.
	 * 
	 * @param folder	대상 폴더 이름.
	 * @return	폴더 이름.
	 */
	public String getParentDir(String folder);
	
	/**
	 * 주어진 이름의 폴더의 이름을 변경시킨다.
	 * 
	 * @param path		변경시킬 대상 폴더 이름.
	 * @param newPath	변경된 새 폴더 이름.
	 */
	public void moveDir(String path, String newPath);
	
	/**
	 * 주어진 이름의 폴더 및 모든 하위 폴더들과 각 폴더에 등록된 모든 데이터세트들을 제거한다.
	 * 
	 * @param folder	대상 폴더 이름.
	 */
	public void deleteDir(String folder);

	/**
	 *  식별자에 해당하는 분석 모듈을 반환한다.
	 *  
	 *  @param id	분석 모듈 식별자.
	 *  @return	분석 모듈
	 *  @throws	AnalysisNotFoundException	식별자에 해당하는 분석 모듈이 존재하지 않는 경우
	 */
	public MarmotAnalysis getAnalysis(String id) throws AnalysisNotFoundException;

	/**
	 *  식별자에 해당하는 분석 모듈을 반환한다.
	 *  식별자에 해당하는 분석 모듈이 없는 경우는 {@code null}을 반환한다.
	 *  
	 *  @param id	분석 모듈 식별자.
	 *  @return	분석 모듈. 분석 모듈이 존재하지 않는 경우는  {@code null}
	 */
	public MarmotAnalysis findAnalysis(String id);
	
	/**
	 * 주어진 분석을 구성요소를 사용하는 상위 분석 모듈을 반환한다.
	 * 상위 분석 모듈이 없는 경우는 {@code null}을 반환한다.
	 * 
	 *  @param id	분석 모듈 식별자.	
	 */
	public CompositeAnalysis findParentAnalysis(String id);
	public List<CompositeAnalysis> getAncestorAnalysisAll(String id);
	public List<MarmotAnalysis> getDescendantAnalysisAll(String id);
	public List<MarmotAnalysis> getAnalysisAll();
	public void addAnalysis(MarmotAnalysis analysis);
	public void deleteAnalysis(String id, boolean recursive);
	public void deleteAnalysisAll();
	public MarmotExecution startAnalysis(MarmotAnalysis analysis) throws MarmotExecutionException;
	public void executeAnalysis(MarmotAnalysis analysis) throws MarmotExecutionException;
	
	public MarmotExecution getMarmotExecution(String id) throws ExecutionNotFoundException;
	public List<MarmotExecution> getMarmotExecutionAll();
	
	/**
	 * {@link Plan} 빌더 객체를 생성한다.
	 * 
	 * @param planName	생성될 Plan의 이름.
	 * @return	Plan 객체.
	 */
	public PlanBuilder planBuilder(String planName);
	
	/**
	 * 주어진 Plan의 수행결과로 생성되는 레코드세트의 스키마를 반환한다.
	 * 
	 * @param plan	Plan 객체.
	 * @param inputSchema	초기 입력 레코드세트 스키마.
	 * @return	레코드세트 스키마.
	 */
	public RecordSchema getOutputRecordSchema(Plan plan, RecordSchema inputSchema);
	
	/**
	 * 주어진 Plan의 수행결과로 생성되는 레코드세트의 스키마를 반환한다.
	 * 
	 * @param plan	Plan 객체.
	 * @return	레코드세트 스키마.
	 */
	public RecordSchema getOutputRecordSchema(Plan plan);
	
	public default MarmotExecution start(Plan plan, ExecutePlanOptions opts)
		throws MarmotExecutionException {
		return startAnalysis(new PlanAnalysis("unnamed", plan, opts));
	}
	public default MarmotExecution start(Plan plan) throws MarmotExecutionException {
		return startAnalysis(new PlanAnalysis("unnamed", plan));
	}
	
	public void execute(Plan plan, ExecutePlanOptions opts)
		throws MarmotExecutionException;
	public default void execute(Plan plan) throws MarmotExecutionException {
		execute(plan, ExecutePlanOptions.DEFAULT);
	}
	
	/**
	 * 주어진 Plan을 MapReduce를 사용하지 않고 수행시킨다.
	 * 
	 * @param plan	수행시킬 실행 계획.
	 * @return	Plan 실행 결과로 생성된 결과 레코드세트.
	 * 			별도의 결과 레코드세트가 생성되지 않은 경우는 {@code null}이 반환된다.
	 */
	public RecordSet executeLocally(Plan plan);
	
	/**
	 * 주어진 Plan을 MapReduce를 사용하지 않고 수행시킨다.
	 * 
	 * @param plan	수행시킬 실행 계획.
	 * @param input	Plan 실행시 제공될 입력 레코드세트.
	 * @return	Plan 실행 결과로 생성된 결과 레코드세트.
	 * 			별도의 결과 레코드세트가 생성되지 않은 경우는 {@code null}이 반환된다.
	 */
	public RecordSet executeLocally(Plan plan, RecordSet input);

	/**
	 * 주어진 Plan을 수행시키고, 그 결과를 반환한다.
	 * <p>
	 * Plan 수행 결과로 생성된 결과 레코드세트의 첫번째 레코드를 반환한다.
	 * 일반적으로 본 메소드는 plan 수행 결과로 단일 레코드가 생성되는 경우 주로 사용된다.
	 * 
	 * @param plan	수행시킬 실행 계획.
	 * @param opts	실행 계획 옵션
	 * @return	Plan 수행 결과로 생성된 레코드세트의 첫번째 레코드.
	 * 				만일 결과 레코드가 생성되지 않은 경우에는 {@link FOption#empty()}.
	 */
	public FOption<Record> executeToRecord(Plan plan, ExecutePlanOptions opts)
		throws MarmotExecutionException;
	public default FOption<Record> executeToRecord(Plan plan) throws MarmotExecutionException {
		return executeToRecord(plan, ExecutePlanOptions.DEFAULT);
	}

	/**
	 * 주어진 Plan을 수행시키고, 그 결과를 반환한다.
	 * <p>
	 * Plan 수행 결과로 생성된 결과 레코드세트의 첫번째 레코드의 첫번째 컬럼 값을
	 * {@link Geometry} 형식으로 변환하여 반환한다.
	 * 일반적으로 본 메소드는 plan 수행 결과로 단일 컬럼으로 구성된 단일 레코드가
	 * 생성되는 경우 주로 사용된다.
	 * 
	 * @param plan	수행시킬 실행 계획.
	 * @param opts	실행 계획 옵션
	 * @return	Plan 수행 결과로 생성된 레코드세트의 첫번째 레코드의 첫번째 컬럼 값.
	 * 				만일 결과 레코드가 생성되지 않은 경우에는 {@link FOption#empty()}.
	 */
	public default FOption<Geometry> executeToGeometry(Plan plan, ExecutePlanOptions opts)
		throws MarmotExecutionException {
		return executeToRecord(plan, opts).map(r -> r.getGeometry(0));
	}
	public default FOption<Geometry> executeToGeometry(Plan plan) throws MarmotExecutionException {
		return executeToGeometry(plan, ExecutePlanOptions.DEFAULT);
	}

	/**
	 * 주어진 Plan을 수행시키고, 그 결과를 반환한다.
	 * <p>
	 * Plan 수행 결과로 생성된 결과 레코드세트의 첫번째 레코드의 첫번째 컬럼 값을
	 * {@link Integer} 형식으로 변환하여 반환한다.
	 * 일반적으로 본 메소드는 plan 수행 결과로 단일 컬럼으로 구성된 단일 레코드가
	 * 생성되는 경우 주로 사용된다.
	 * 
	 * @param plan	수행시킬 실행 계획.
	 * @param opts	실행 계획 옵션
	 * @return	Plan 수행 결과로 생성된 레코드세트의 첫번째 레코드의 첫번째 컬럼 값.
	 * 				만일 결과 레코드가 생성되지 않은 경우에는 {@link FOption#empty()}.
	 */
	public default FOption<Integer> executeToInt(Plan plan, ExecutePlanOptions opts)
		throws MarmotExecutionException {
		return executeToRecord(plan, opts).map(r -> r.getInt(0));
	}
	public default FOption<Integer> executeToInt(Plan plan) throws MarmotExecutionException {
		return executeToInt(plan, ExecutePlanOptions.DEFAULT);
	}

	/**
	 * 주어진 Plan을 수행시키고, 그 결과를 반환한다.
	 * <p>
	 * Plan 수행 결과로 생성된 결과 레코드세트의 첫번째 레코드의 첫번째 컬럼 값을
	 * {@link Long} 형식으로 변환하여 반환한다.
	 * 일반적으로 본 메소드는 plan 수행 결과로 단일 컬럼으로 구성된 단일 레코드가
	 * 생성되는 경우 주로 사용된다.
	 * 
	 * @param plan	수행시킬 실행 계획.
	 * @param opts	실행 계획 옵션
	 * @return	Plan 수행 결과로 생성된 레코드세트의 첫번째 레코드의 첫번째 컬럼 값.
	 * 				만일 결과 레코드가 생성되지 않은 경우에는 {@link FOption#empty()}.
	 */
	public default FOption<Long> executeToLong(Plan plan, ExecutePlanOptions opts)
		throws MarmotExecutionException {
		return executeToRecord(plan, opts).map(r -> r.getLong(0));
	}
	public default FOption<Long> executeToLong(Plan plan) throws MarmotExecutionException {
		return executeToLong(plan, ExecutePlanOptions.DEFAULT);
	}

	/**
	 * 주어진 Plan을 수행시키고, 그 결과를 반환한다.
	 * <p>
	 * Plan 수행 결과로 생성된 결과 레코드세트의 첫번째 레코드의 첫번째 컬럼 값을
	 * {@link Double} 형식으로 변환하여 반환한다.
	 * 일반적으로 본 메소드는 plan 수행 결과로 단일 컬럼으로 구성된 단일 레코드가
	 * 생성되는 경우 주로 사용된다.
	 * 
	 * @param plan	수행시킬 실행 계획.
	 * @param opts	실행 계획 옵션
	 * @return	Plan 수행 결과로 생성된 레코드세트의 첫번째 레코드의 첫번째 컬럼 값.
	 * 				만일 결과 레코드가 생성되지 않은 경우에는 {@link FOption#empty()}.
	 */
	public default FOption<Double> executeToDouble(Plan plan, ExecutePlanOptions opts)
		throws MarmotExecutionException {
		return executeToRecord(plan, opts).map(r -> r.getDouble(0));
	}
	public default FOption<Double> executeToDouble(Plan plan) throws MarmotExecutionException {
		return executeToDouble(plan, ExecutePlanOptions.DEFAULT);
	}

	/**
	 * 주어진 Plan을 수행시키고, 그 결과를 반환한다.
	 * <p>
	 * Plan 수행 결과로 생성된 결과 레코드세트의 첫번째 레코드의 첫번째 컬럼 값을
	 * {@link String} 형식으로 변환하여 반환한다.
	 * 일반적으로 본 메소드는 plan 수행 결과로 단일 컬럼으로 구성된 단일 레코드가
	 * 생성되는 경우 주로 사용된다.
	 * 
	 * @param plan	수행시킬 실행 계획.
	 * @param opts	실행 계획 옵션
	 * @return	Plan 수행 결과로 생성된 레코드세트의 첫번째 레코드의 첫번째 컬럼 값.
	 * 				만일 결과 레코드가 생성되지 않은 경우에는 {@link FOption#empty()}.
	 */
	public default FOption<String> executeToString(Plan plan, ExecutePlanOptions opts)
		throws MarmotExecutionException {
		return executeToRecord(plan, opts).map(r -> r.getString(0));
	}
	public default FOption<String> executeToString(Plan plan) throws MarmotExecutionException {
		return executeToString(plan, ExecutePlanOptions.DEFAULT);
	}
	
	@SuppressWarnings("unchecked")
	public default <T> FOption<T> executeToSingle(Plan plan, ExecutePlanOptions opts)
		throws MarmotExecutionException {
		return executeToRecord(plan, opts).map(r -> (T)r.get(0));
	}
	public default <T> FOption<T> executeToSingle(Plan plan) throws MarmotExecutionException {
		return executeToSingle(plan, ExecutePlanOptions.DEFAULT);
	}
	
	public RecordSet executeToRecordSet(Plan plan, ExecutePlanOptions opts);
	public default RecordSet executeToRecordSet(Plan plan) {
		return executeToRecordSet(plan, ExecutePlanOptions.DEFAULT);
	}
	
	public RecordSet executeToStream(String id, Plan plan);

	public RecordSchema getProcessOutputRecordSchema(String processId, Map<String,String> params);
	public void executeProcess(String processId, Map<String,String> params);
	
	public void createKafkaTopic(String topic, boolean force);
	
	public RecordSet readMarmotFile(String path) throws MarmotFileNotFoundException;
	public void copyToHdfsFile(String path, InputStream stream,
								FOption<Long> blockSize, FOption<String> codecName)
		throws IOException;
	public void deleteHdfsFile(String path) throws IOException;
}
