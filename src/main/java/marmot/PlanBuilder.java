package marmot;

import static marmot.optor.geo.SpatialRelation.ALL;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import com.google.protobuf.ByteString;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

import marmot.geo.GeoClientUtils;
import marmot.optor.AggregateFunction;
import marmot.optor.JoinOptions;
import marmot.optor.ParseCsvOptions;
import marmot.optor.StoreAsCsvOptions;
import marmot.optor.geo.SpatialRelation;
import marmot.optor.geo.SquareGrid;
import marmot.optor.geo.advanced.InterpolationMethod;
import marmot.optor.geo.advanced.LISAWeight;
import marmot.plan.GeomOpOptions;
import marmot.plan.Group;
import marmot.plan.JdbcConnectOptions;
import marmot.plan.LoadJdbcTableOptions;
import marmot.plan.LoadOptions;
import marmot.plan.PredicateOptions;
import marmot.plan.SpatialJoinOptions;
import marmot.proto.GeometryColumnInfoProto;
import marmot.proto.GeometryProto;
import marmot.proto.SerializedProto;
import marmot.proto.TypeCodeProto;
import marmot.proto.optor.ArcClipProto;
import marmot.proto.optor.ArcSpatialJoinProto;
import marmot.proto.optor.ArcSplitProto;
import marmot.proto.optor.AssignSquareGridCellProto;
import marmot.proto.optor.AssignUidProto;
import marmot.proto.optor.AttachGeoHashProto;
import marmot.proto.optor.AttachQuadKeyProto;
import marmot.proto.optor.BinarySpatialIntersectionProto;
import marmot.proto.optor.BinarySpatialIntersectsProto;
import marmot.proto.optor.BinarySpatialUnionProto;
import marmot.proto.optor.BreakLineStringProto;
import marmot.proto.optor.BufferTransformProto;
import marmot.proto.optor.CentroidTransformProto;
import marmot.proto.optor.ClusterChroniclesProto;
import marmot.proto.optor.ConsumeByGroupProto;
import marmot.proto.optor.DefineColumnProto;
import marmot.proto.optor.DissolveProto;
import marmot.proto.optor.DistinctProto;
import marmot.proto.optor.DropEmptyGeometryProto;
import marmot.proto.optor.DropProto;
import marmot.proto.optor.EstimateIDWProto;
import marmot.proto.optor.EstimateKernelDensityProto;
import marmot.proto.optor.ExpandProto;
import marmot.proto.optor.FilterSpatiallyProto;
import marmot.proto.optor.FlattenGeometryProto;
import marmot.proto.optor.GroupConsumerProto;
import marmot.proto.optor.HashJoinProto;
import marmot.proto.optor.InterpolateSpatiallyProto;
import marmot.proto.optor.LISAWeightProto;
import marmot.proto.optor.ListReducerProto;
import marmot.proto.optor.LoadCustomTextFileProto;
import marmot.proto.optor.LoadDataSetProto;
import marmot.proto.optor.LoadGetisOrdGiProto;
import marmot.proto.optor.LoadHashJoinProto;
import marmot.proto.optor.LoadHexagonGridFileProto;
import marmot.proto.optor.LoadHexagonGridFileProto.GridBoundsProto;
import marmot.proto.optor.LoadJdbcTableProto;
import marmot.proto.optor.LoadLocalMoransIProto;
import marmot.proto.optor.LoadMarmotFileProto;
import marmot.proto.optor.LoadSpatialClusterIndexFileProto;
import marmot.proto.optor.LoadSpatialClusteredFileProto;
import marmot.proto.optor.LoadSpatialIndexJoinProto;
import marmot.proto.optor.LoadSquareGridFileProto;
import marmot.proto.optor.LoadTextFileProto;
import marmot.proto.optor.LoadWholeFileProto;
import marmot.proto.optor.OperatorProto;
import marmot.proto.optor.ParseCsvProto;
import marmot.proto.optor.PickTopKProto;
import marmot.proto.optor.PlanProto;
import marmot.proto.optor.ProjectProto;
import marmot.proto.optor.PutSideBySideProto;
import marmot.proto.optor.QueryDataSetProto;
import marmot.proto.optor.RankProto;
import marmot.proto.optor.ReduceGeometryPrecisionProto;
import marmot.proto.optor.ReducerProto;
import marmot.proto.optor.RunPlanProto;
import marmot.proto.optor.SampleProto;
import marmot.proto.optor.ScriptFilterProto;
import marmot.proto.optor.ShardProto;
import marmot.proto.optor.SortProto;
import marmot.proto.optor.SpatialBlockJoinProto;
import marmot.proto.optor.SpatialDifferenceJoinProto;
import marmot.proto.optor.SpatialIntersectionJoinProto;
import marmot.proto.optor.SpatialKnnInnerJoinProto;
import marmot.proto.optor.SpatialKnnOuterJoinProto;
import marmot.proto.optor.SpatialOuterJoinProto;
import marmot.proto.optor.SpatialReduceJoinProto;
import marmot.proto.optor.SpatialSemiJoinProto;
import marmot.proto.optor.SplitGeometryProto;
import marmot.proto.optor.StoreAndReloadProto;
import marmot.proto.optor.StoreAsCsvProto;
import marmot.proto.optor.StoreAsHeapfileProto;
import marmot.proto.optor.StoreIntoDataSetProto;
import marmot.proto.optor.StoreIntoJdbcTableProto;
import marmot.proto.optor.StoreIntoKafkaTopicProto;
import marmot.proto.optor.StoreKeyedDataSetProto;
import marmot.proto.optor.TakeProto;
import marmot.proto.optor.TakeReducerProto;
import marmot.proto.optor.TeeProto;
import marmot.proto.optor.ToGeometryPointProto;
import marmot.proto.optor.ToXYCoordinatesProto;
import marmot.proto.optor.TransformByGroupProto;
import marmot.proto.optor.TransformCrsProto;
import marmot.proto.optor.UnarySpatialIntersectionProto;
import marmot.proto.optor.UpdateProto;
import marmot.proto.optor.ValidateGeometryProto;
import marmot.proto.optor.ValueAggregateReducersProto;
import marmot.protobuf.PBUtils;
import marmot.support.PBSerializable;
import marmot.type.DataType;
import utils.Utilities;
import utils.func.FOption;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PlanBuilder {
	private final PlanProto.Builder m_builder = PlanProto.newBuilder();
	
	public PlanBuilder(String name) {
		m_builder.setName(name);
	}
	
	public PlanBuilder add(OperatorProto proto) {
		m_builder.addOperators(proto);
		return this;
	}
	
	public PlanBuilder add(PBSerializable<?> serializable) {
		return add(OperatorProto.newBuilder()
								.setSerialized(serializable.serialize())
								.build());
	}
	
	public String getName() {
		return m_builder.getName();
	}
	
	public Plan build() {
		return Plan.fromProto(m_builder.build());
	}

	/**
	 * 주어진 식별자들의 데이터세트들을 읽어 {@link RecordSet}를 적재하는 연산을 추가한다.
	 * <p>
	 * 데이터세트 적재시 추가 정보를  {@link LoadOptions}을 활용하여 전달한다.
	 * 
	 * @param dsIdList	적재할 데이터세트 이름 리스트.
	 * @param opts		옵션 리스트.
	 * @return	연산이 추가된 {@link PlanBuilder} 객체.
	 */
	public PlanBuilder load(Iterable<String> dsIdList, LoadOptions opts) {
		Utilities.checkNotNullArguments(dsIdList, "dataset list is null");
		Utilities.checkNotNullArgument(opts, "opts is null");
		
		LoadDataSetProto proto = LoadDataSetProto.newBuilder()
												.addAllDsIds(dsIdList)
												.setOptions(opts.toProto())
												.build();
		return add(OperatorProto.newBuilder()
								.setLoadDataset(proto)
								.build());
	}

	/**
	 * 주어진 식별자의 데이터세트를 읽어 {@link RecordSet}를 적재하는 연산을 추가한다.
	 * <p>
	 * 데이터세트 적재시 추가 정보를  {@link LoadOptions}을 활용하여 전달한다.
	 * 
	 * @param dsId	대상 데이터세트 이름.
	 * @param opts	옵션 리스트.
	 * @return	연산이 추가된 {@link PlanBuilder} 객체.
	 */
	public PlanBuilder load(String dsId, LoadOptions opts) {
		Utilities.checkNotNullArgument(dsId, "dsId is null");
		Utilities.checkNotNullArgument(opts, "opts is null");
		
		return load(Arrays.asList(dsId), opts);
	}

	/**
	 * 주어진 식별자의 데이터세트를 읽어 {@link RecordSet}를 적재하는 연산을 추가한다.
	 * 
	 * @param dsId	대상 데이터세트 이름.
	 * @return	연산이 추가된 {@link PlanBuilder} 객체.
	 */
	public PlanBuilder load(String... dsId) {
		return load(Arrays.asList(dsId), LoadOptions.DEFAULT);
	}
	
	/*
	 *	주요 레코드 세트 적재 연산자들
	 */
	
	/**
	 * 주어진 경로에 해당하는 marmot 파일들을 읽는 {@link RecordSet}을 생성하는 연산을 추가한다.
	 * 
	 * @param pathes	읽을 Marmot 파일들의 경로명 리스트.
	 * @return		작업이 추가된 {@code PlanBuilder} 객체.
	 */
	public PlanBuilder loadMarmotFile(String... pathes) {
		Utilities.checkNotNullArguments(pathes, "pathes is null");
		
		return loadMarmotFile(Arrays.asList(pathes), LoadOptions.DEFAULT);
	}
	
	/**
	 * 주어진 경로에 해당하는 marmot 파일들을 읽는 {@link RecordSet}을 생성하는 연산을 추가한다.
	 * 
	 * @param pathes	읽을 Marmot 파일들의 경로명 리스트.
	 * @param opts		활용 option 정보.
	 * 				최속한 1보다 크거나 같아야한다.
	 * @return		작업이 추가된 {@code PlanBuilder} 객체.
	 */
	public PlanBuilder loadMarmotFile(Iterable<String> pathes, LoadOptions opts) {
		Utilities.checkNotNullArgument(pathes, "pathes is null");
		Utilities.checkNotNullArgument(opts, "opts is null");
		
		LoadMarmotFileProto load = LoadMarmotFileProto.newBuilder()
													.addAllPaths(pathes)
													.setOptions(opts.toProto())
													.build();
		return add(OperatorProto.newBuilder()
								.setLoadMarmotfile(load)
								.build());
	}
	
	public PlanBuilder loadWholeFile(Iterable<String> pathes, LoadOptions opts) {
		Utilities.checkNotNullArgument(pathes, "pathes is null");
		Utilities.checkNotNullArgument(opts, "opts is null");
		
		LoadWholeFileProto load = LoadWholeFileProto.newBuilder()
													.addAllPaths(pathes)
													.setOptions(opts.toProto())
													.build();
		return add(OperatorProto.newBuilder()
								.setLoadWholeFile(load)
								.build());
	}
	public PlanBuilder loadWholeFile(String... pathes) {
		return loadWholeFile(Arrays.asList(pathes), LoadOptions.DEFAULT);
	}
	public PlanBuilder loadWholeFile(String path, LoadOptions opts) {
		return loadWholeFile(Arrays.asList(path), opts);
	}
	
	public PlanBuilder loadSpatialClusteredFile(String dsId, String clusterCols) {
		Utilities.checkNotNullArgument(dsId, "dsId is null");
		Utilities.checkNotNullArgument(clusterCols, "clusterCols is null");
		
		LoadSpatialClusteredFileProto load = LoadSpatialClusteredFileProto.newBuilder()
													.setDataset(dsId)
													.setRange(PBUtils.toProto(new Envelope()))
													.setQuery(ALL.toStringExpr())
													.setClusterColsExpr(clusterCols)
													.build();
		return add(OperatorProto.newBuilder()
								.setLoadSpatialClusterFile(load)
								.build());
	}

	/**
	 * 주어진 경로의 텍스트 파일을 읽어 RecordSet을 로드하는 작업을 추가한다.
	 * 
	 * @param pathes	읽을 텍스트 파일의 경로.
	 * @param opts		활용 option 정보.
	 * @return 명령이 추가된 {@code PlanBuilder} 객체.
	 */
	public PlanBuilder loadTextFile(Iterable<String> pathes, LoadOptions opts) {
		Utilities.checkNotNullArgument(pathes, "pathes is null");
		
		LoadTextFileProto load = LoadTextFileProto.newBuilder()
													.addAllPaths(pathes)
													.setOptions(opts.toProto())
													.build();
		return add(OperatorProto.newBuilder()
								.setLoadTextfile(load)
								.build());
		
	}
	public PlanBuilder loadTextFile(String... pathes) {
		return loadTextFile(Arrays.asList(pathes), LoadOptions.DEFAULT);
	}
	
	public PlanBuilder loadCustomTextFile(String path) {
		Utilities.checkNotNullArgument(path, "path is null");
		
		LoadOptions opts = LoadOptions.DEFAULT;
		LoadCustomTextFileProto load = LoadCustomTextFileProto.newBuilder()
															.setPath(path)
															.setOptions(opts.toProto())
															.build();
		
		return add(OperatorProto.newBuilder()
								.setLoadCustomTextfile(load)
								.build());
	}
	
	/**
	 * JDBC 인터페이스를 이용하여 주어진 위치의 DBMS의 테이블 레코드를 읽어
	 * {@link RecordSet}를 구성하는 연산을 추가한다.
	 * 
	 * @param tableName 	접속 대상 테이블 이름.
	 * @param jdbcOpts		JDBC option 정보
	 * @param opts			활용 옵션 리스트.
	 * @return 작업이 추가된 {@link PlanBuilder} 객체.
	 */
	public PlanBuilder loadJdbcTable(String tableName, JdbcConnectOptions jdbcOpts,
									LoadJdbcTableOptions opts) {
		Utilities.checkNotNullArgument(tableName, "tableName is null");
		Utilities.checkNotNullArgument(jdbcOpts, "JdbcConnectOptions is null");
		Utilities.checkNotNullArgument(opts, "LoadJdbcTableOptions is null");
		
		LoadJdbcTableProto load = LoadJdbcTableProto.newBuilder()
													.setTableName(tableName)
													.setJdbcOptions(jdbcOpts.toProto())
													.setOptions(opts.toProto())
													.build();
		
		return add(OperatorProto.newBuilder()
								.setLoadJdbcTable(load)
								.build());
	}
	
//	/**
//	 * JMS 인터페이스를 이용하여 주어진 위치의 큐에 저장된 레코드를 읽어
//	 * {@link RecordSet}를 구성하는 명령을 추가한다.
//	 * 
//	 * @param brokerUrl		JMS 프로토콜을 통해 접근할 JMS 브로커 URL.
//	 * @param queueName 	접속 대상 큐 이름.
//	 * @param recordSchema	생성될 레코드 세트의 스키마.
//	 * @return 작업이 추가된 {@link PlanBuilder} 객체.
//	 */
//	public PlanBuilder loadJmsQueue(String brokerUrl, String queueName, RecordSchema recordSchema) {
//		return add(LoadJmsQueue.builder()
//								.brokerUrl(brokerUrl)
//								.queue(queueName)
//								.recordSchema(recordSchema)
//								.build());
//	}
//
	//***********************************************************************
	//	주요 레코드 세트 저장 연산자들
	//***********************************************************************
	/**
	 * 주어진 경로의 Marmot 파일에 앞선 명령을 통해 생성된 레코드 세트를 저장하는 연산을 추가한다.
	 * 
	 * @param path	저장될 HDFS 파일 경로명.
	 * @return 명령이 추가된 {@link PlanBuilder} 객체.
	 */
	public PlanBuilder storeMarmotFile(String path) {
		Utilities.checkNotNullArgument(path, "path is null");
		
		StoreAsHeapfileProto store = StoreAsHeapfileProto.newBuilder()
														.setPath(path)
														.build();
		return add(OperatorProto.newBuilder()
								.setStoreAsHeapfile(store)
								.build());
	}
	
	public PlanBuilder tee(String path) {
		Utilities.checkNotNullArgument(path, "path is null");
		
		TeeProto tee = TeeProto.newBuilder()
								.setPath(path)
								.build();
		return add(OperatorProto.newBuilder()
								.setTee(tee)
								.build());
	}
	
	/**
	 * 입력 {@link RecordSet}을 주어진 CSV 형식의 텍스트 파일로 저장하는 연산을 작업계획에 추가한다.
	 * {@code RecordSet}의 각 레코드는 하나의 라인으로 기록되고,
	 * 레코드의 각 컬럼은 인자로 제공되는 (delim)으로 분리된다.
	 * 
	 * @param path	저장될 파일의 경로명.
	 * @param opts	활용 option들
	 * @return 연산이 추가된 {@link PlanBuilder} 객체.
	 */
	public PlanBuilder storeAsCsv(String path, StoreAsCsvOptions opts) {
		Utilities.checkNotNullArgument(path, "path is null");
		
		StoreAsCsvProto store = StoreAsCsvProto.newBuilder()
												.setPath(path)
												.setOptions(opts.toProto())
												.build();
		return add(OperatorProto.newBuilder()
								.setStoreAsCsv(store)
								.build());
	}
	public PlanBuilder storeAsCsv(String path) {
		return storeAsCsv(path, StoreAsCsvOptions.DEFAULT());
	}
	
	/**
	 * JDBC 인터페이스를 이용하여 주어진 위치의 DBMS의 테이블에 저장하는 연산을 추가한다.
	 * 
	 * @param tableName 	접속 대상 테이블 이름.
	 * @param jdbcOpts		JDBC option 정보
	 * @param valuesExpr	삽입될 레코드 표현식
	 * @return 작업이 추가된 {@link PlanBuilder} 객체.
	 */
	public PlanBuilder storeIntoJdbcTable(String tableName, JdbcConnectOptions jdbcOpts,
										String valuesExpr) {
		Utilities.checkNotNullArgument(tableName, "tableName is null");
		Utilities.checkNotNullArgument(valuesExpr, "valuesExpr is null");
		Utilities.checkNotNullArgument(jdbcOpts, "JdbcConnectOptions is null");
		
		StoreIntoJdbcTableProto store = StoreIntoJdbcTableProto.newBuilder()
												.setTableName(tableName)
												.setJdbcOptions(jdbcOpts.toProto())
												.setValuesExpr(valuesExpr)
												.build();
		return add(OperatorProto.newBuilder()
								.setStoreIntoJdbcTable(store)
								.build());
	}
	
	public PlanBuilder storeIntoKafkaTopic(String topic) {
		StoreIntoKafkaTopicProto store = StoreIntoKafkaTopicProto.newBuilder()
																.setTopic(topic)
																.build();
		return add(OperatorProto.newBuilder()
								.setStoreIntoKafkaTopic(store)
								.build());
	}
	
//	/**
//	 * JMS 인터페이스를 이용하여 주어진 위치의 큐에 저장하는 명령을 추가한다.
//	 * 
//	 * @param brokerUrl		JMS 프로토콜을 통해 접근할 JMS 브로커 URL.
//	 * @param queueName 	접속 대상 큐 이름.
//	 * @return 작업이 추가된 {@link PlanBuilder} 객체.
//	 */
//	public PlanBuilder storeIntoQueue(String brokerUrl, String queueName) {
//		return add(StoreIntoJmsQueue.builder()
//									.brokerUrl(brokerUrl)
//									.queue(queueName)
//									.build());
//	}
//	
//
//	//***********************************************************************
//	//	주요 레코드 세트 연산자들
//	//***********************************************************************
	
	/**
	 * 본 {@code PlanBuilder}에 필터 연산을 추가한다.
	 * <p>
	 * 주어진 {@code predicate}는 주어진 레코드 세트에 포함된 모든 레코드에 적용되며,
	 * MVEL 문법을 사용하여 기술되어야 하고, 수행 결과는 boolean 타입이어야 한다.
	 * 본 연산 수행으로 생성되는 레코드 세트는 {@code predicate} 수행 결과 {@code true}가
	 * 반환된 레코드들로 구성된다. 
	 * 
	 * @param predicate	필터 연산의 조건절.
	 * @return 연산이 추가된 {@link PlanBuilder} 객체.
	 */
	public PlanBuilder filter(String predicate) {
		Utilities.checkNotNullArgument(predicate, "predicate is null");
		
		return filter(RecordScript.of(predicate));
	}

	/**
	 * 본 {@code PlanBuilder}에 필터 명령을 추가한다.
	 * <p>
	 * 주어진 {@code predicate}는 주어진 레코드 세트에 포함된 모든 레코드에 적용되며,
	 * MVEL 문법을 사용하여 기술되어야 하고, 수행 결과는 boolean 타입이어야 한다.
	 * 본 명령 수행으로 생성되는 레코드 세트는 {@code predicate} 수행 결과 {@code true}가
	 * 반환된 레코드들로 구성된다.
	 * 
	 * @param predicate		필터 조건절.
	 * @return 명령이 추가된 {@link PlanBuilder} 객체.
	 */
	public PlanBuilder filter(RecordScript predicate) {
		Utilities.checkNotNullArgument(predicate, "predicate is null");
		
		ScriptFilterProto filter = ScriptFilterProto.newBuilder()
													.setPredicate(predicate.toProto())
													.build();
		return add(OperatorProto.newBuilder()
								.setFilterScript(filter)
								.build());
	}

	/**
	 * 본 {@code PlanBuilder}에 입력 레코드에서 지정된 컬럼만으로 구성된 레코드들로
	 * 이루어진 레코드 세트를 생성하는 연산을 추가한다.
	 * 
	 * @param columnSelection	생성될 레코드에 포함될 컬럼 표현식.
	 * @return 연산이 추가된 {@link PlanBuilder} 객체.
	 */
	public PlanBuilder project(String columnSelection) {
		Utilities.checkNotNullArgument(columnSelection, "columnSelection is null");
		
		ProjectProto project = ProjectProto.newBuilder()
											.setColumnExpr(columnSelection)
											.build();
		return add(OperatorProto.newBuilder()
								.setProject(project)
								.build());
	}
	
	/**
	 * 주어진 갱신 표현식을 이용하여, 레코드 세트에 포함된 모든 레코드에 반영시킨다.
	 * 
	 * @param updateExpr	갱신 표현식
	 * @return 연산이 추가된 {@link PlanBuilder} 객체.
	 */
	public PlanBuilder update(String updateExpr) {
		return update(RecordScript.of(updateExpr));
	}
	/**
	 * 주어진 갱신 표현식을 이용하여, 레코드 세트에 포함된 모든 레코드에 반영시킨다.
	 * 
	 * @param expr	갱신 표현식
	 * @return 연산이 추가된 {@link PlanBuilder} 객체.
	 */
	public PlanBuilder update(RecordScript expr) {
		Utilities.checkNotNullArgument(expr, "update expression is null");
		
		UpdateProto update = UpdateProto.newBuilder()
										.setScript(expr.toProto())
										.build();
		
		return add(OperatorProto.newBuilder()
								.setUpdate(update)
								.build());
	}

	/**
	 * 주어진 레코드 세트에 포함된 레코드에 주어진 이름의 컬럼을 추가한다.
	 * 추가된 컬럼의 값은 {@code null}로 설정된다.
	 * 
	 * @param colDecl	추가될 컬럼의 이름과 타입 정보
	 * @return 연산이 추가된 {@link PlanBuilder} 객체.
	 */
	public PlanBuilder defineColumn(String colDecl) {
		Utilities.checkNotNullArgument(colDecl, "colDecl is null");
		
		DefineColumnProto op = DefineColumnProto.newBuilder()
												.setColumnDecl(colDecl)
												.build();
		return add(OperatorProto.newBuilder()
								.setDefineColumn(op)
								.build());
	}

	/**
	 * 주어진 레코드 세트에 포함된 레코드에 주어진 이름의 컬럼을 추가하고,
	 * 해당 컬럼의 초기값을 설정한다.
	 * 
	 * @param colDecl	추가될 컬럼의 이름과 타입 정보
	 * @param colInit	추가된 컬럼의 초기값 설정식
	 * @return 연산이 추가된 {@link PlanBuilder} 객체.
	 */
	public PlanBuilder defineColumn(String colDecl, String colInit) {
		Utilities.checkNotNullArgument(colDecl, "colDecl is null");
		Utilities.checkNotNullArgument(colInit, "colInit is null");
		
		return defineColumn(colDecl, RecordScript.of(colInit));
	}

	/**
	 * 주어진 레코드 세트에 포함된 레코드에 주어진 이름의 컬럼을 추가하고,
	 * 해당 컬럼의 초기값을 설정한다.
	 * 
	 * @param colDecl	추가될 컬럼의 이름과 타입 정보
	 * @param colInitScript	추가된 컬럼의 초기값 설정식
	 * @return 연산이 추가된 {@link PlanBuilder} 객체.
	 */
	public PlanBuilder defineColumn(String colDecl, RecordScript colInitScript) {
		Utilities.checkNotNullArgument(colDecl, "colDecl is null");
		Utilities.checkNotNullArgument(colInitScript, "colInitScript is null");
		
		DefineColumnProto op = DefineColumnProto.newBuilder()
											.setColumnDecl(colDecl)
											.setColumnInitializer(colInitScript.toProto())
											.build();
		return add(OperatorProto.newBuilder()
								.setDefineColumn(op)
								.build());
	}

	public PlanBuilder expand(String colDecls) {
		Utilities.checkNotNullArgument(colDecls, "colDecls is null");
		
		ExpandProto expand = ExpandProto.newBuilder()
										.setColumnDecls(colDecls)
										.build();
		return add(OperatorProto.newBuilder()
								.setExpand(expand)
								.build());
	}

	public PlanBuilder expand(String colDecls, String colInit) {
		Utilities.checkNotNullArgument(colDecls, "colDecls is null");
		Utilities.checkNotNullArgument(colInit, "colInit is null");
		
		return expand(colDecls, RecordScript.of(colInit));
	}

	public PlanBuilder expand(String colDecls, RecordScript colInitScript) {
		Utilities.checkNotNullArgument(colDecls, "colDecls is null");
		Utilities.checkNotNullArgument(colInitScript, "colInitScript is null");
		
		ExpandProto expand = ExpandProto.newBuilder()
										.setColumnDecls(colDecls)
										.setColumnInitializer(colInitScript.toProto())
										.build();
		return add(OperatorProto.newBuilder()
								.setExpand(expand)
								.build());
	}

	/**
	 * 입력 레코드의 지정된 컬럼의 CSV 값을 파싱한 결과를 레코드에 추가한다.
	 * 
	 * @param csvColumn	CSV 값을 갖고 있는 컬럼 이름.
	 * @param opts		CSV 파싱 옵션 정보
	 * @return 연산이 추가된 {@link PlanBuilder} 객체.
	 */
	public PlanBuilder parseCsv(String csvColumn, ParseCsvOptions opts) {
		Utilities.checkNotNullArgument(csvColumn, "CSV column is null");
		Utilities.checkNotNullArgument(opts, "ParseCsvOptions is null");
		
		ParseCsvProto proto = ParseCsvProto.newBuilder()
											.setCsvColumn(csvColumn)
											.setOptions(opts.toProto())
											.build();
		return add(OperatorProto.newBuilder()
								.setParseCsv(proto)
								.build());
	}

	/**
	 * 입력 레코드 세트에 포함된 모든 레코드에 유일 식별자 컬럼을 추가한다.
	 * <p>
	 * 부여된 식별자는 {@code long} 타입으로 정의된다. 식별자는 일반적으로 0부터 순차적으로
	 * 커지는 값이 부여되지만, 반드시 1씩 증가되는 값이 부여되지는 않는다.
	 * 
	 * @param uidColName	식별자 컬럼 이름.
	 * @return 연산이 추가된 {@link PlanBuilder} 객체.
	 */
	public PlanBuilder assignUid(String uidColName) {
		Utilities.checkNotNullArgument(uidColName, "uid column is null");
		
		AssignUidProto assign = AssignUidProto.newBuilder()
												.setUidColumn(uidColName)
												.build();
		return add(OperatorProto.newBuilder()
								.setAssignUid(assign)
								.build());
	}
	
	/**
	 * 본 {@code PlanBuilder}에 Limit 연산을 추가한다.
	 * <p>
	 * Limit 연산은 입력 레코드 세트에 포함된 레코드들 중에서 주어진 갯수만큼의 레코드들로만 구성된
	 * 레코드 세트를 생성하는 연산이다.
	 * 일반적으로는 입력 레코드 세트에 ㅍ포함된 레코드들 중에서 처음 'count' 개의 레코드들로 구성된
	 * 레코드 세트를 구성하지만, Map-Reduce로 실행되는 경우는 순서 관계가 무관하게 주어진 갯수의
	 * 레코드들로 레코드 세트를 구성할 수 있다.
	 * 
	 * @param count	포함시킬 레코드의 갯수.
	 * @return 연산이 추가된 {@link PlanBuilder} 객체.
	 */
	public PlanBuilder take(long count) {
		Utilities.checkArgument(count >= 0, "invalid drop count: count=" + count);
		
		TakeProto drop = TakeProto.newBuilder().setCount(count).build();
		return add(OperatorProto.newBuilder()
								.setTake(drop)
								.build());
	}
	
	/**
	 * 본 {@code PlanBuilder}에 Skip 연산을 추가한다.
	 * <p>
	 * Skip은 입력 레코드 세트에 포함된 레코드들 중에서 주어진 갯수만큼의 레코드들을 제외한
	 * 나머지들로  구성된 레코드 세트를 생성하는 연산이다.
	 * 또한 {@code Skip} 연산은 지역 연산으로 MapReduce 방식으로 수행되지 않는다.
	 * 
	 * @param count	누락시킬 레코드의 갯수.
	 * @return 연산이 추가된 {@link PlanBuilder} 객체.
	 */
	public PlanBuilder drop(long count) {
		Utilities.checkArgument(count >= 0, "invalid drop count: count=" + count);
		
		DropProto drop = DropProto.newBuilder()
									.setCount(count)
									.build();
		return add(OperatorProto.newBuilder()
								.setDrop(drop)
								.build());
	}

	/**
	 * 본 {@code PlanBuilder}에 Sample 연산을 추가한다.
	 * <p>
	 * Sample은 입력 레코드들 중에서 주어진 확률로 레코드들 만으로 구성된 레코드 세트를 생성한다.
	 * 
	 * @param sampleRatio	샘플 비율.
	 * @return 연산이 추가된 {@link PlanBuilder} 객체.
	 */
	public PlanBuilder sample(double sampleRatio) {
		Utilities.checkArgument(Double.compare(sampleRatio, 0) > 0
									&& Double.compare(sampleRatio, 0) <= 1,
									"invalid sample ratio: ratio=" + sampleRatio);
		
		SampleProto sample = SampleProto.newBuilder()
										.setSampleRatio(sampleRatio)
										.build();
		return add(OperatorProto.newBuilder()
								.setSample(sample)
								.build());
	}
	
	public PlanBuilder clusterChronicles(String inputColumn, String outputColumn,
										String threshold) {
		ClusterChroniclesProto op = ClusterChroniclesProto.newBuilder()
														.setInputColumn(inputColumn)
														.setOutputColumn(outputColumn)
														.setThreshold(threshold)
														.build();
		return add(OperatorProto.newBuilder()
								.setClusterChronicles(op)
								.build());
	}
	
	/**
	 * 본 {@code PlanBuilder}에 Sort 연산을 추가한다.
	 * <p>
	 * Sort은 입력 레코드 세트에 포함된 모든 레코드들을 인자로 전달된
	 * 정렬 키 컬럼({@code sortColSpecs})을 기준으로 정렬한 레코드 세트를 생성한다.
	 * 정렬 키 컬럼 {@code sortColSpecs} 인자는 정렬에 사용될 컬럼과 정렬 순서를 
	 * 정의하며 다음과 같은 방식으로 기술된다.
	 * <pre>{@code (<column_name> ':' ('A'|'D')? ',')* (<column_name> ':' ('A'|'D')?)
	 * }</pre>.
	 * 정렬 순서는 위와 같이 'A'또는 'D'로 표현되고, 'A'인 경우는 해당 컬럼으로는 오름차순으로,
	 * 'D'인 경우는 내림차순을 의미한다. 별도의 정렬 순서가 기술되지 않은 경우는 오름차순으로 가정한다.
	 * 예를들어 {@code "id:A,name:D"}인 경우는 'id' 컬럼 값으로는 오름차순으로, 'name' 컬럼 값으로는
	 * 내림차순으로 정렬시키는 것을 의미한다.
	 * 
	 * @param sortColSpecs	정렬 키 컬럼 기술.
	 * @return 연산이 추가된 {@link PlanBuilder} 객체.
	 */
	public PlanBuilder sort(String sortColSpecs) {
		Utilities.checkNotNullArgument(sortColSpecs, "sortColSpecs is null");
		
		SortProto sort = SortProto.newBuilder()
									.setSortColumns(sortColSpecs)
									.build();
		return add(OperatorProto.newBuilder()
								.setSort(sort)
								.build());
	}

	/**
	 * 본 {@code PlanBuilder}에 하나 이상의 집계함수를 수행하는 연산을 추가한다.
	 * <p>
	 * 본 연산은 입력 레코드 세트에 포함된 모든 레코드에 주어진 집계 연산 ({@code aggregators})들을
	 * 적용하여 하나의 레코드를 생성하는 연산이다.
	 * 
	 * @param aggrFuncs	적용할 집계 함수 리스트.
	 * @return 연산이 추가된 {@link PlanBuilder} 객체.
	 */
	public PlanBuilder aggregate(Iterable<AggregateFunction> aggrFuncs) {
		Utilities.checkNotNullArguments(aggrFuncs, "empty AggregateFunction");
		
		ValueAggregateReducersProto varp
							= FStream.from(aggrFuncs)
									.map(AggregateFunction::toProto)
									.foldLeft(ValueAggregateReducersProto.newBuilder(),
												(builder,aggr) -> builder.addAggregate(aggr))
									.build();
		ReducerProto reducer = ReducerProto.newBuilder()
											.setValAggregates(varp)
											.build();
		return add(OperatorProto.newBuilder()
								.setReduce(reducer)
								.build());
	}
	/**
	 * 본 {@code PlanBuilder}에 하나 이상의 집계함수를 수행하는 연산을 추가한다.
	 * <p>
	 * 본 연산은 입력 레코드 세트에 포함된 모든 레코드에 주어진 집계 연산 ({@code aggregators})들을
	 * 적용하여 하나의 레코드를 생성하는 연산이다.
	 * 
	 * @param aggrFuncs	적용할 집계 함수 리스트.
	 * @return 연산이 추가된 {@link PlanBuilder} 객체.
	 */
	public PlanBuilder aggregate(AggregateFunction... aggrFuncs) {
		Utilities.checkNotNullArguments(aggrFuncs, "empty AggregateFunction");
		
		return aggregate(Arrays.asList(aggrFuncs));
	}

	/**
	 * {@link Group} 작업으로 그룹핑된 각 레코드 그룹에 대해
	 * 집계 함수를 적용시켜 결과 레코드 세트를 출력하는 작업을 추가한다.
	 * 
	 * @param group		그룹 지정 정보
	 * @param aggrs	각 레코드 그룹에 적용할 집계함수 리스트. 계함수 리스트. 
	 * @return 작업이 추가된 {@link PlanBuilder} 객체.
	 */
	public PlanBuilder aggregateByGroup(Group group, AggregateFunction... aggrs) {
		Utilities.checkNotNullArgument(group, "group is null");
		Utilities.checkNotNullArguments(aggrs, "empty AggregateFunction");
		
		return aggregateByGroup(group, Arrays.asList(aggrs));
	}
	/**
	 * {@link Group} 작업으로 그룹핑된 각 레코드 그룹에 대해
	 * 집계 함수를 적용시켜 결과 레코드 세트를 출력하는 작업을 추가한다.
	 * 
	 * @param group		그룹 지정 정보
	 * @param aggrs	각 레코드 그룹에 적용할 집계함수 리스트. 계함수 리스트. 
	 * @return 작업이 추가된 {@link PlanBuilder} 객체.
	 */
	public PlanBuilder aggregateByGroup(Group group, List<AggregateFunction> aggrs) {
		Utilities.checkNotNullArgument(group, "group is null");
		Utilities.checkNotNullArguments(aggrs, "empty AggregateFunction");
		
		ValueAggregateReducersProto varp = FStream.from(aggrs)
													.map(AggregateFunction::toProto)
													.foldLeft(ValueAggregateReducersProto.newBuilder(),
																(b,f) -> b.addAggregate(f))
													.build();
		ReducerProto reducer = ReducerProto.newBuilder()
											.setValAggregates(varp)
											.build();
		return reduceByGroup(group, reducer);
	}
	
	/**
	 * 입력 레코드들을 {@code group}을 기준으로 그룹핑하고, 각 그룹에 포함된
	 * 레코드들 중에서 주어진 갯수만큼만 뽑은 레코드 세트를 생성시킨다.
	 * <p>
	 * 그룹에 포함된 레코드의 수가 주어진 {@code count}보다 작은 경우는 해당 레코드들만
	 * 선택된다.
	 * 
	 * @param group	그룹 기준
	 * @param count	각 그룹에서 선택할 레코드의 수
	 * @return 작업이 추가된 {@link PlanBuilder} 객체.
	 */
	public PlanBuilder takeByGroup(Group group, int count) {
		Utilities.checkNotNullArgument(group, "group is null");
		Utilities.checkArgument(count > 0, "count > 0");
		
		TakeReducerProto take = TakeReducerProto.newBuilder().setCount(count).build();
		ReducerProto reducer = ReducerProto.newBuilder()
											.setTake(take)
											.build();
		return reduceByGroup(group, reducer);
	}
	
	/**
	 * 입력 레코드들을 {@code group}을 기준으로 그룹핑하고, 각 그룹별로 레코드의 순서가
	 * 바뀐 레코드 세트를 생성시킨다.
	 * 
	 * @param group	그룹 기준
	 * @return 작업이 추가된 {@link PlanBuilder} 객체.
	 */
	public PlanBuilder listByGroup(Group group) {
		Utilities.checkNotNullArgument(group, "group is null");
		
		ListReducerProto list = ListReducerProto.newBuilder().build();
		ReducerProto reducer = ReducerProto.newBuilder()
											.setList(list)
											.build();
		return reduceByGroup(group, reducer);
	}
	
	public PlanBuilder runPlanByGroup(Group group, Plan plan) {
		RunPlanProto run = RunPlanProto.newBuilder()
										.setPlan(plan.toProto())
										.build();
		ReducerProto reducer = ReducerProto.newBuilder()
											.setRunPlan(run)
											.build();
		return reduceByGroup(group, reducer);
	}
	
	public PlanBuilder reduceToSingleRecordByGroup(Group group, RecordSchema outSchema,
													String tagCol, String valueCol) {
		PutSideBySideProto put = PutSideBySideProto.newBuilder()
												.setOutputSchema(outSchema.toProto())
												.setTagColumn(tagCol)
												.setValueColumn(valueCol)
												.build();
		ReducerProto reducer = ReducerProto.newBuilder()
											.setPutSideBySide(put)
											.build();
		return reduceByGroup(group, reducer);
	}
	
	public PlanBuilder applyByGroup(Group group, PBSerializable<?> serializable) {
		SerializedProto proto = serializable.serialize();
		return reduceByGroup(group, ReducerProto.newBuilder().setReducer(proto).build());
	}

	public PlanBuilder applyByGroup(Group group, Serializable serializable) {
		return applyByGroup(group, PBUtils.serializeJava(serializable));
	}
	
	private PlanBuilder reduceByGroup(Group group, ReducerProto reducer) {
		TransformByGroupProto transform = TransformByGroupProto.newBuilder()
															.setGrouper(group.toProto())
															.setTransform(reducer)
															.build();
		return add(OperatorProto.newBuilder()
								.setTransformByGroup(transform)
								.build());
	}
	
	public PlanBuilder storeByGroup(Group group, String rootPath, StoreDataSetOptions opts) {
		StoreKeyedDataSetProto store = StoreKeyedDataSetProto.newBuilder()
															.setRootPath(rootPath)
															.setOptions(opts.toProto())
															.build();
		GroupConsumerProto consumer = GroupConsumerProto.newBuilder()
														.setStore(store)
														.build();
		ConsumeByGroupProto proto = ConsumeByGroupProto.newBuilder()
														.setGrouper(group.toProto())
														.setConsumer(consumer)
														.build();
		return add(OperatorProto.newBuilder()
								.setConsumeByGroup(proto)
								.build());
	}

	/**
	 * 입력 레코드 세트에 포함된 레코드들에서 중복된 레코드를 제거한 레코드세트를 출력하는
	 * 명령을 추가한다.
	 * 
	 * @param distinctKeyCols	중복 레코드 여부를 판단할 키 컬럼 이름 (리스트).
	 * @param nworkers		중복 옵션 객체. 별도의 옵션이 없는 경우는 {@code null}을 사용.
	 * @return 작업이 추가된 {@link PlanBuilder} 객체.
	 */
	public PlanBuilder distinct(String distinctKeyCols, int nworkers) {
		DistinctProto distinct = DistinctProto.newBuilder()
										.setKeyColumns(distinctKeyCols)
										.setWorkerCount(nworkers)
										.build();
		return add(OperatorProto.newBuilder()
								.setDistinct(distinct)
								.build());
	}

	/**
	 * 입력 레코드 세트에 포함된 레코드들에서 중복된 레코드를 제거한 레코드세트를 출력하는
	 * 명령을 추가한다.
	 * 
	 * @param distinctKeyCols	중복 레코드 여부를 판단할 키 컬럼 이름 (리스트).
	 * @return 작업이 추가된 {@link PlanBuilder} 객체.
	 */
	public PlanBuilder distinct(String distinctKeyCols) {
		DistinctProto distinct = DistinctProto.newBuilder()
										.setKeyColumns(distinctKeyCols)
										.build();
		return add(OperatorProto.newBuilder()
								.setDistinct(distinct)
								.build());
	}
	
	/**
	 * 본 {@code PlanBuilder}에 Rank 연산을 추가한다.
	 * <p>
	 * Rank은 입력 레코드 세트에 포함된 모든 레코드들을 인자로 전달된
	 * 정렬 키 컬럼({@code orderKeyColSpecs})을 기준으로 정렬하고, 각 레코드의 정렬 순서를
	 * 주어진 인자 {@code rankColName}로 지정된 컬럼이 추가된 레코드들로 구성된 레코드 세트를
	 * 생성하는 연산이다.
	 * 정렬 키 컬럼 {@code orderKeyColSpecs} 인자는 정렬에 사용될 컬럼과 정렬 순서를 
	 * 정의하며 다음과 같은 방식으로 기술된다.
	 * <pre>{@code (<column_name> ':' ('A'|'D')? ',')* (<column_name> ':' ('A'|'D')?)
	 * }</pre>.
	 * 정렬 순서는 위와 같이 'A'또는 'D'로 표현되고, 'A'인 경우는 해당 컬럼으로는 오름차순으로,
	 * 'D'인 경우는 내림차순을 의미한다. 별도의 정렬 순서가 기술되지 않은 경우는 오름차순으로 가정한다.
	 * 예를들어 {@code "id:A,name:D"}인 경우는 'id' 컬럼 값으로는 오름차순으로, 'name' 컬럼 값으로는
	 * 내림차순으로 정렬시키는 것을 의미한다.
	 * 
	 * @param compareKeyCols	정렬 키 컬럼 기술.
	 * @param rankColName	순서가 저장될 컬럼 이름.
	 * @return 연산이 추가된 {@link PlanBuilder} 객체.
	 */
	public PlanBuilder rank(String compareKeyCols, String rankColName) {
		Utilities.checkNotNullArgument(compareKeyCols, "compare key columns name is null");
		Utilities.checkNotNullArgument(rankColName, "output rank column name is null");
		
		RankProto pick = RankProto.newBuilder()
								.setSortKeyColumns(compareKeyCols)
								.setRankColumn(rankColName)
								.build();
		return add(OperatorProto.newBuilder()
								.setRank(pick)
								.build());
	}
	
	/**
	 * 본 {@code PlanBuilder}에 PickTopK 연산을 추가한다.
	 * <p>
	 * PickTopK은 입력 레코드 세트에 포함된 모든 레코드들을 인자로 전달된
	 * 정렬 키 컬럼({@code sortKeyColSpecs})을 기준으로 정렬하여 주어진 갯수 ({@code topK})의
	 * 레코드들로 구성된 레코드 세트를 생성하는 연산이다.
	 * 만일 동일 rank의 레코드가 존재하여 {@code topK} 개의 레코드를 선택할 수 없는 경우는
	 * 동일 rank의 레코드들 중 임의로 선택하여 강제로 {@code topK} 갯수의 레코드 세트를 생성한다.
	 * <p>
	 * 정렬 키 컬럼 {@code sortKeyColSpecs} 인자는 정렬에 사용될 컬럼과 정렬 순서를 
	 * 정의하며 다음과 같은 방식으로 기술된다.
	 * <pre>{@code (<column_name> ':' ('A'|'D')? ',')* (<column_name> ':' ('A'|'D')?)
	 * }</pre>.
	 * 정렬 순서는 위와 같이 'A'또는 'D'로 표현되고, 'A'인 경우는 해당 컬럼으로는 오름차순으로,
	 * 'D'인 경우는 내림차순을 의미한다. 별도의 정렬 순서가 기술되지 않은 경우는 오름차순으로 가정한다.
	 * 예를들어 {@code "id:A,name:D"}인 경우는 'id' 컬럼 값으로는 오름차순으로, 'name' 컬럼 값으로는
	 * 내림차순으로 정렬시키는 것을 의미한다.
	 * @param sortKeyColSpecs	정렬 키 컬럼 기술.
	 * @param topK	선택할 레코드 갯수의 최대 값.
	 * @return 연산이 추가된 {@link PlanBuilder} 객체.
	 */
	public PlanBuilder pickTopK(String sortKeyColSpecs, int topK) {
		Utilities.checkNotNullArgument(sortKeyColSpecs, "sort key columns");
		Utilities.checkArgument(topK >= 1, "topK >= 1");
		
		PickTopKProto pick = PickTopKProto.newBuilder()
										.setSortKeyColumns(sortKeyColSpecs)
										.setTopK(topK)
										.build();
		return add(OperatorProto.newBuilder()
								.setPickTopK(pick)
								.build());
	}
	
//	/**
//	 * 본 {@code PlanBuilder}에 PickTopRankK 연산을 추가한다.
//	 * <p>
//	 * PickTopRankK은 입력 레코드 세트에 포함된 모든 레코드들을 인자로 전달된
//	 * 정렬 키 컬럼({@code orderKeyColSpecs})을 기준으로 정렬하여 주어진 갯수 ({@code topK})의
//	 * 레코드들로 구성된 레코드 세트를 생성하는 연산이다.
//	 * 만일 동일 rank의 레코드가 존재하여 {@code topK} 개의 레코드를 선택할 수 없는 경우는
//	 * 동일 rank의 레코드들을 모두 포함시키기 때문에 결과 레코드들은 {@code topK}보다 많을 수 있다. 
//	 * <p>
//	 * 정렬 키 컬럼 {@code orderKeyColSpecs} 인자는 정렬에 사용될 컬럼과 정렬 순서를 
//	 * 정의하며 다음과 같은 방식으로 기술된다.
//	 * <pre>{@code (<column_name> ':' ('A'|'D')? ',')* (<column_name> ':' ('A'|'D')?)
//	 * }</pre>.
//	 * 정렬 순서는 위와 같이 'A'또는 'D'로 표현되고, 'A'인 경우는 해당 컬럼으로는 오름차순으로,
//	 * 'D'인 경우는 내림차순을 의미한다. 별도의 정렬 순서가 기술되지 않은 경우는 오름차순으로 가정한다.
//	 * 예를들어 {@code "id:A,name:D"}인 경우는 'id' 컬럼 값으로는 오름차순으로, 'name' 컬럼 값으로는
//	 * 내림차순으로 정렬시키는 것을 의미한다.
//	 * 
//	 * @param orderKeyColSpecs	정렬 키 컬럼 기술.
//	 * @param topK	선택할 레코드 갯수의 최대 값.
//	 * @return 연산이 추가된 {@link PlanBuilder} 객체.
//	 */
//	public PlanBuilder pickTopRankK(String orderKeyColSpecs, int topK);
	
	public PlanBuilder loadHashJoin(String leftDataSet, String leftJoinCols,
										String rightDataSet, String rightJoinCols,
										String outputColumnExpr, JoinOptions opts) {
		Utilities.checkNotNullArgument(leftDataSet,  "left dataset id is null");
		Utilities.checkNotNullArgument(rightDataSet,  "right dataset id is null");
		Utilities.checkNotNullArgument(leftJoinCols,  "left join columns are null");
		Utilities.checkNotNullArgument(rightJoinCols,  "right join columns are null");
		Utilities.checkNotNullArgument(outputColumnExpr, "output columns is null");
		Utilities.checkNotNullArgument(opts, "JoinOptions is null");
		
		opts = (opts == null) ? JoinOptions.INNER_JOIN : opts;
		
		LoadHashJoinProto load = LoadHashJoinProto.newBuilder()
												.setLeftDataset(leftDataSet)
												.setLeftJoinColumns(leftJoinCols)
												.setRightDataset(rightDataSet)
												.setRightJoinColumns(rightJoinCols)
												.setOutputColumnsExpr(outputColumnExpr)
												.setJoinOptions(opts.toProto())
												.build();
		return add(OperatorProto.newBuilder()
								.setLoadHashJoin(load)
								.build());
	}
	
	/**
	 * 입력 레코드세트와 인자로 주어진 레코드 세트와 조인을 수행하는 명령을 추가한다.
	 * <p>
	 * Inner 조인 외에 다른 조인을 사용하거나, 조인 작업에 사용될 reducer의 갯수를
	 * 명시적으로 지정하기 위해서는 <code>optSetter</code>를 통해 지정한다.
	 * 예를들어 64개의 reducer를 사용하여 left-outer 조인을 사용하기 위해서는
	 * 다음과 같은 방법을 사용한다.
	 * <pre>{@code
	 * Plan plan = marmot.planBuilder()
	 * 							.load(...)
	 * 							.join("a", param, "b", "*,param.*",
	 * 								new JoinOptions().joinType(JoinType.LEFT_OUTER_JOIN))
	 * 												.workerCount(64)
	 * 							.store(...)
	 * 							.build();
	 * }</pre>
	 * 
	 * @param inputJoinCols	입력 레코드의 컬럼 중에서 조인 컬럼으로 사용될 컬럼 이름들. 
	 * @param paramDataSet	조인될 대상 인자 데이터 세트.
	 * @param paramJoinCols	인자 레코드 컬럼 중에서 조인 컬럼으로 사용될 컬럼 이름들.
	 * @param outputColumnExpr	조인 결과 레코드에 포함될 컬럼 리스트. 
	 * @param opts			추가 조인 옵션. 추가의 옵션이 필요없는 경우는 null을 사용함.
	 * @return	연산이 추가된 {@link PlanBuilder} 객체.
	 */
	public PlanBuilder hashJoin(String inputJoinCols, String paramDataSet,
								String paramJoinCols, String outputColumnExpr,
								JoinOptions opts) {
		Utilities.checkNotNullArgument(inputJoinCols, "input join columns are null");
		Utilities.checkNotNullArgument(paramDataSet, "parameter DataSet id is null");
		Utilities.checkNotNullArgument(paramJoinCols, "parameter join columns are null");
		Utilities.checkNotNullArgument(outputColumnExpr, "output column expression is null");
		Utilities.checkNotNullArgument(opts, "JoinOptions is null");
		
		HashJoinProto join = HashJoinProto.newBuilder()
										.setJoinColumns(inputJoinCols)
										.setParamDataset(paramDataSet)
										.setParamColumns(paramJoinCols)
										.setOutputColumnsExpr(outputColumnExpr)
										.setJoinOptions(opts.toProto())
										.build();
		return add(OperatorProto.newBuilder()
								.setHashJoin(join)
								.build());
	}
	
	public PlanBuilder shard(int partCount) {
		Utilities.checkArgument(partCount > 0, "invalid partition count: " + partCount);
		
		ShardProto shard = ShardProto.newBuilder()
									.setPartCount(partCount)
									.build();
		return add(OperatorProto.newBuilder()
								.setShard(shard)
								.build());
	}
	
	public PlanBuilder reload(LoadOptions opts) {
		StoreAndReloadProto reload = StoreAndReloadProto.newBuilder()
														.setOptions(opts.toProto())
														.build();
		return add(OperatorProto.newBuilder()
								.setStoreAndReload(reload)
								.build());
	}
	public PlanBuilder reload() {
		return reload(LoadOptions.DEFAULT);
	}
	
	//***********************************************************************
	//***********************************************************************
	//	공간 정보 관련 연산자들
	//***********************************************************************
	//***********************************************************************
	
	/**
	 * 주어진 이름의 데이터세트에 속한 레코드들 중에서 주어진 공간 객체와 조건을 만족시키는 레코드들로
	 * 구성된 레코드 세트를 적재시키는 연산을 추가한다.
	 * 
	 * @param dsId	읽을 대상 데이터세트 이름.
	 * @param key	조건 대상 공간 객체.
	 * @param opts	옵션 리스트
	 * @return		작업이 추가된 {@link PlanBuilder} 객체.
	 */
	public PlanBuilder query(String dsId, Geometry key, PredicateOptions opts) {
		Utilities.checkNotNullArgument(dsId, "input dataset id");
		Utilities.checkNotNullArgument(key, "key is null");
				
		QueryDataSetProto query = QueryDataSetProto.newBuilder()
													.setDsId(dsId)
													.setKeyGeometry(PBUtils.toProto(key))
													.setOptions(opts.toProto())
													.build();
		return add(OperatorProto.newBuilder()
								.setQueryDataset(query)
								.build());
	}
	public PlanBuilder query(String dsId, Geometry key) {
		return query(dsId, key, PredicateOptions.DEFAULT);
	}
	public PlanBuilder query(String dsId, Envelope bounds, PredicateOptions opts) {
		Utilities.checkNotNullArgument(bounds, "key bounds");
		
		return query(dsId, GeoClientUtils.toPolygon(bounds), opts);
	}
	public PlanBuilder query(String dsId, Envelope bounds) {
		return query(dsId, bounds, PredicateOptions.DEFAULT);
	}
	
	public PlanBuilder query(String dsId, String keyDsId, PredicateOptions opts) {
		Utilities.checkNotNullArgument(dsId, "input dataset id");
		Utilities.checkNotNullArgument(keyDsId, "key dataset id");
				
		QueryDataSetProto query = QueryDataSetProto.newBuilder()
													.setDsId(dsId)
													.setKeyDataset(keyDsId)
													.setOptions(opts.toProto())
													.build();
		
		return add(OperatorProto.newBuilder()
								.setQueryDataset(query)
								.build());
	}
	public PlanBuilder query(String dsId, String keyDsId) {
		return query(dsId, keyDsId, PredicateOptions.DEFAULT);
	}

	/**
	 * 본 {@code PlanBuilder}에 입력 레코드 세트에 포함된 레코드들 중에서
	 * 주어진 공간 객체 컬럼와 교집합이 존재하는
	 * 레코드들로 구성된 레코드 세트를 생성하는 연산을 추가한다.
	 * <p>
	 * 레코드 내의 공간 객체와 인자로 주어진 공간 객체 {@code key}는 동일한 SRID 이어야 한다. 
	 * 
	 * @param geomCol	입력 레코드에서 사용될 공간 객체 컬럼 이름.
	 * @param rel		조건 공간 연산 관계
	 * @param key		교집합 여부를 검사할 공간 객체.
	 * @param opts		옵션 리스트
	 * @return 명령이 추가된 {@code PlanBuilder} 객체.
	 */
	public PlanBuilder filterSpatially(String geomCol, SpatialRelation rel, Geometry key,
										PredicateOptions opts) {
		Utilities.checkNotNullArgument(geomCol, "geometry column name");
		Utilities.checkNotNullArgument(rel, "SpatialRelation");
		Utilities.checkNotNullArgument(key, "key geometry");
		Utilities.checkNotNullArgument(opts, "PredicateOption");
		
		GeometryProto keyProto = PBUtils.toProto(key);
		FilterSpatiallyProto op = FilterSpatiallyProto.newBuilder()
														.setGeometryColumn(geomCol)
														.setSpatialRelation(rel.toStringExpr())
														.setKeyGeometry(keyProto)
														.setOptions(opts.toProto())
														.build();

		return add(OperatorProto.newBuilder()
								.setFilterSpatially(op)
								.build());
	}
	public PlanBuilder filterSpatially(String geomCol, SpatialRelation rel, Geometry key) {
		return filterSpatially(geomCol, rel, key, PredicateOptions.DEFAULT);
	}

	/**
	 * 본 {@code PlanBuilder}에 입력 레코드 세트에 포함된 레코드들 중에서
	 * 주어진 공간 객체 컬럼와 교집합이 존재하는
	 * 레코드들로 구성된 레코드 세트를 생성하는 연산을 추가한다.
	 * <p>
	 * 레코드 내의 공간 객체와 인자로 주어진 공간 객체 {@code key}는 동일한 SRID 이어야 한다. 
	 * 
	 * @param geomCol	입력 레코드에서 사용될 공간 객체 컬럼 이름.
	 * @param rel		조건 공간 연산 관계
	 * @param keyDsId	교집합 여부를 검사할 공간 객체가 저장된 데이터세트 식별자.
	 * @param opts		옵션 리스트
	 * @return 명령이 추가된 {@code PlanBuilder} 객체.
	 */
	public PlanBuilder filterSpatially(String geomCol, SpatialRelation rel, String keyDsId,
										PredicateOptions opts) {
		Utilities.checkNotNullArgument(geomCol, "geometry column name");
		Utilities.checkNotNullArgument(rel, "SpatialRelation");
		Utilities.checkNotNullArgument(keyDsId, "key dataset id");
		Utilities.checkNotNullArgument(opts, "PredicateOption");
		
		FilterSpatiallyProto op = FilterSpatiallyProto.newBuilder()
													.setGeometryColumn(geomCol)
													.setSpatialRelation(rel.toStringExpr())
													.setKeyDataset(keyDsId)
													.setOptions(opts.toProto())
													.build();

		return add(OperatorProto.newBuilder()
								.setFilterSpatially(op)
								.build());
	}
	public PlanBuilder filterSpatially(String geomCol, SpatialRelation rel, String keyDsId) {
		return filterSpatially(geomCol, rel, keyDsId, PredicateOptions.DEFAULT);
	}
	
	/**
	 * 주어진 데이터세트 클러스터 정보를 반환한다.
	 * <p>
	 * 클러스터 정보는 레코드 세트 형태로 반환되며, 각 클러스터마다 하나의 레코드로
	 * 구성된다.
	 * 각 레코드는 다음과 같은 컬럼으로 구성된다.
	 * <ol>
	 * 	<li> pack_id: 클러스터를 저장한 클러스터 파일 식별.
	 * 	<li> block_no: 클러스터 파일 내에 해당 클러스터의 순번.
	 * 	<li> quad_key: 클러스터 영역에 해당하는 quad-key 식별자.
	 * 	<li> tile_bounds: 클러스터의 타일 영역.
	 * 	<li> data_bounds: 클러스터에 저장된 데이터의 MBR 영역.
	 * 	<li> count: 클러스터에 저장된 데이터의 캣수.
	 * 	<li> owned_count: 클러스터에 저장된 데이터 중에서 본 클러스터 소유의 레코드 갯수.
	 * 	<li> start: 클러스터 파일 내 저장된 클러스터의 시작 오프셋 (바이트 단위)
	 * 	<li> length: 클러스터 파일 내 저장된 클러스터의 크기 (바이트 단위)
	 * </ol>
	 * 
	 * @param dsId	클러스터 정보를 확인할 데이터세트 식별자.
	 * @return 연산이 추가된 {@link PlanBuilder} 객체.
	 */
	public PlanBuilder loadSpatialClusterIndexFile(String dsId) {
		Utilities.checkNotNullArgument(dsId, "dataset id is null");
		
		LoadSpatialClusterIndexFileProto load = LoadSpatialClusterIndexFileProto.newBuilder()
																			.setDataset(dsId)
																			.build();
		return add(OperatorProto.newBuilder()
				.setLoadSpatialClusterIndexFile(load)
				.build());
	}
	
	/**
	 * 입력 {@link RecordSet}을 주어진 이름의 데이터세트로 저장하는 연산을 추가한다.
	 * <p>
	 * 저장될 레어어의 공간 객체 컬럼은 레코드 세트의 컬럼들 중 공간 타입을 갖는 컬럼으로 간주한다.
	 * 만일 복수의 컬럼이 공간 타입을 갖는 경우는 컬럼 이름이 'the_geom'인 컬럼을 공간 컬럼으로
	 * 간주하고, 그런 이름의 컬럼이 없는 경우는 {@link RecordSetException} 예외를 발생시킨다.
	 * 
	 * @param dsId	저장할 데이터세트 식별자.
	 * @return 연산이 추가된 {@link PlanBuilder} 객체.
	 */
	public PlanBuilder store(String dsId) {
		Utilities.checkNotNullArgument(dsId, "dataset id");

		StoreIntoDataSetProto store = StoreIntoDataSetProto.newBuilder()
															.setId(dsId)
															.build();
		
		return add(OperatorProto.newBuilder()
								.setStoreIntoDataset(store)
								.build());
	}
	
	public PlanBuilder arcSplit(String geomCol, String splitDsId, String splitKey,
								String outDir, StoreDataSetOptions opts) {
		Utilities.checkNotNullArgument(geomCol, "geometry column is null");
		Utilities.checkNotNullArgument(splitDsId, "split dataset is null");
		Utilities.checkNotNullArgument(splitKey, "split key is null");
		Utilities.checkNotNullArgument(outDir, "output dataset directory is null");
		Utilities.checkNotNullArgument(opts, "StoreDataSetOptions is null");
		
		ArcSplitProto proto = ArcSplitProto.newBuilder()
											.setGeomColumn(geomCol)
											.setSplitDataset(splitDsId)
											.setSplitKey(splitKey)
											.setOutputDatasetDir(outDir)
											.setOptions(opts.toProto())
											.build();
		return add(OperatorProto.newBuilder()
								.setArcSplit(proto)
								.build());
	}

	/**
	 * 사각형 그리드 레코드 세트를 생성하는 명령을 추가한다.
	 * 
	 * @param grid		생성할 그리드 정보.
	 * @param nparts	그리드 생성 작업 mapper 갯수
	 * @return	명령이 추가된 {@link PlanBuilder} 객체.
	 */
	public PlanBuilder loadGrid(SquareGrid grid, int nparts) {
		Utilities.checkNotNullArgument(grid, "SquareGrid is null");
		Utilities.checkArgument(nparts > 0, "invalid part count: count=" + nparts);

		LoadSquareGridFileProto load = LoadSquareGridFileProto.newBuilder()
																.setGrid(grid.toProto())
																.setSplitCount(nparts)
																.build();
		return add(OperatorProto.newBuilder()
								.setLoadSquareGridfile(load)
								.build());
	}
	public PlanBuilder loadGrid(SquareGrid grid) {
		Utilities.checkNotNullArgument(grid, "SquareGrid is null");

		LoadSquareGridFileProto load = LoadSquareGridFileProto.newBuilder()
																.setGrid(grid.toProto())
																.build();
		return add(OperatorProto.newBuilder()
								.setLoadSquareGridfile(load)
								.build());
	}
	
	/**
	 * 육각형 그리드 레코드 세트를 생성하는 명령을 추가한다.
	 * @param bounds	생성할 그리드의 전체 영역.
	 * @param srid		생성할 그리드의 좌표 체계.
	 * @param sideLen	생성될 hexagon의 한쪽 면의 길이
	 * @param nparts	MapReduce 처리 중 생성할 파티션(reducer)의 갯수
	 * 
	 * @return	명령이 추가된 {@link PlanBuilder} 객체.
	 */
	public PlanBuilder loadHexagonGridFile(Envelope bounds, String srid, double sideLen,
											int nparts) {
		Utilities.checkNotNullArgument(bounds != null && !bounds.isNull(), "grid bounds is null");
		Utilities.checkNotNullArgument(srid != null , "srid is null");
		Utilities.checkArgument(Double.compare(sideLen, 0) > 0,
								"invalid side-length: len=" + sideLen);
		Utilities.checkArgument(nparts > 0, "invalid partition count: count=" + nparts);

		GridBoundsProto boundsProto = GridBoundsProto.newBuilder()
												.setBounds(PBUtils.toProto(bounds))
												.setSrid(srid)
												.build();
		LoadHexagonGridFileProto load
					= LoadHexagonGridFileProto.newBuilder()
												.setBounds(boundsProto)
												.setSideLength(sideLen)
												.setPartCount(nparts)
												.build();
		return add(OperatorProto.newBuilder()
								.setLoadHexagonGridfile(load)
								.build());
	}
	
	public PlanBuilder loadHexagonGridFile(String dataset, double sideLen, int nparts) {
		Utilities.checkNotNullArgument(dataset, "dataset is null");
		Utilities.checkArgument(Double.compare(sideLen, sideLen) > 0,
								"invalid side-length: len=" + sideLen);
		Utilities.checkArgument(nparts > 0, "invalid partition count: count=" + nparts);

		LoadHexagonGridFileProto load
					= LoadHexagonGridFileProto.newBuilder()
											.setDataset(dataset)
											.setSideLength(sideLen)
											.setPartCount(nparts)
											.build();
		return add(OperatorProto.newBuilder()
								.setLoadHexagonGridfile(load)
								.build());
	}
	
	/**
	 * 입력 레코드 세트에 포함된 각 레코드에 주어진 크기의 사각형 Grid 셀 정보를 부여한다.
	 * 
	 * 출력 레코드에는 '{@code cell_id}', '{@code cell_pos}', 그리고 '{@code cell_geom}'
	 * 컬럼이 추가된다. 각 컬럼 내용은 각각 다음과 같다.
	 * <dl>
	 * 	<dt>cell_id</dt>
	 * 	<dd>부여된 그리드 셀의 고유 식별자. {@code long} 타입</dd>
	 * 	<dt>cell_pos</dt>
	 * 	<dd>부여된 그리드 셀의 x/y 좌표 . {@code GridCell} 타입</dd>
	 * 	<dt>cell_geom</dt>
	 * 	<dd>부여된 그리드 셀의 공간 객체 . {@code Polygon} 타입</dd>
	 * </dl>
	 * 
	 * 입력 레코드의 공간 객체가 {@code null}이거나 {@link Geometry#isEmpty()}가
	 * {@code true}인 경우, 또는 공간 객체의 위치가 그리드 전체 영역 밖에 있는 레코드의
	 * 처리는 {@code ignoreOutside} 인자에 따라 처리된다.
	 * 만일 {@code ignoreOutside}가 {@code false}인 경우 영역 밖 레코드의 경우는
	 * 출력 레코드 세트에 포함되지만, '{@code cell_id}', '{@code cell_pos}',
	 * '{@code cell_geom}'의 컬럼 값은 {@code null}로 채워진다.
	 * 
	 * @param geomCol	Grid cell 생성에 사용할 공간 컬럼 이름.
	 * @param grid		생성할 격자 정보.
	 * @param assignOutside	입력 공간 객체가 주어진 그리드 전체 영역에서 벗어난 경우
	 * 						무시 여부. 무시하는 경우는 결과 레코드 세트에 포함되지 않음.
	 * @return	명령이 추가된 {@link PlanBuilder} 객체.
	 */
	public PlanBuilder assignGridCell(String geomCol, SquareGrid grid,
											boolean assignOutside) {
		Utilities.checkNotNullArgument(geomCol, "geometry column is null");
		Utilities.checkNotNullArgument(grid, "SquareGrid is null");

		AssignSquareGridCellProto assign
							= AssignSquareGridCellProto.newBuilder()
													.setGeometryColumn(geomCol)
													.setGrid(grid.toProto())
													.setAssignOutside(assignOutside)
													.build();
		return add(OperatorProto.newBuilder()
								.setAssignSquareGridCell(assign)
								.build());
	}
	
	//***********************************************************************
	//***********************************************************************
	//	공간 정보 관계 연산자들
	//***********************************************************************
	//***********************************************************************
	
	public PlanBuilder dropEmptyGeometry(String geomCol, PredicateOptions opts) {
		DropEmptyGeometryProto op = DropEmptyGeometryProto.newBuilder()
														.setGeometryColumn(geomCol)
														.setOptions(opts.toProto())
														.build();
		
		return add(OperatorProto.newBuilder()
								.setDropEmptyGeometry(op)
								.build());
	}
	public PlanBuilder dropEmptyGeometry(String geomCol) {
		return dropEmptyGeometry(geomCol, PredicateOptions.DEFAULT);
	}
	
	/**
	 * 본 {@code PlanBuilder}에 입력 레코드 세트에 포함된 레코드들 중에서
	 * 주어진 두 개의 공간 객체 컬럼들의 공간 객체를 읽어 이들 교집합을 생성하여
	 * 지정된 컬럼에 저장하는 연산을 추가한다.
	 * <p>의
	 * 레코드 내의 두 공간 객체들은 서로 동일한 SRID 이어야 한다. 
	 * 
	 * @param leftGeomCol	중첩여부 검사에 참여할 왼쪽 컬럼 이름.
	 * @param rightGeomCol	중첩여부 검사에 참여할 오른쪽 컬럼 이름.
	 * @param	opts		옵션 리스트.
	 * @return 명령이 추가된 {@code PlanBuilder} 객체.
	 */
	public PlanBuilder intersectsBinary(String leftGeomCol, String rightGeomCol,
										PredicateOptions opts) {
		Utilities.checkNotNullArgument(leftGeomCol, "leftGeomCol is null");
		Utilities.checkNotNullArgument(rightGeomCol, "rightGeomCol is null");

		BinarySpatialIntersectsProto op = BinarySpatialIntersectsProto.newBuilder()
															.setLeftGeometryColumn(leftGeomCol)
															.setRightGeometryColumn(rightGeomCol)
															.setOptions(opts.toProto())
															.build();
		
		return add(OperatorProto.newBuilder()
								.setBinarySpatialIntersects(op)
								.build());
	}
	public PlanBuilder intersectsBinary(String leftGeomCol, String rightGeomCol) {
		return intersectsBinary(leftGeomCol, rightGeomCol, PredicateOptions.DEFAULT);
	}

	//***********************************************************************
	//***********************************************************************
	//	공간 정보 조작 연산자들
	//***********************************************************************
	//***********************************************************************
	
	public PlanBuilder attachGeoHash(String geomCol, String hashCol, boolean asLong) {
		Utilities.checkNotNullArgument(geomCol, "geomCol is null");
		Utilities.checkNotNullArgument(hashCol, "hash column is null");
		
		AttachGeoHashProto attach = AttachGeoHashProto.newBuilder()
														.setGeometryColumn(geomCol)
														.setHashColumn(hashCol)
														.setAsLong(asLong)
														.build();
		return add(OperatorProto.newBuilder()
								.setAttachGeohash(attach)
								.build());
	}
	public PlanBuilder attachGeoHash(String geomCol, String hashCol) {
		Utilities.checkNotNullArgument(geomCol, "geomCol is null");
		Utilities.checkNotNullArgument(hashCol, "hash column is null");
		
		AttachGeoHashProto attach = AttachGeoHashProto.newBuilder()
														.setGeometryColumn(geomCol)
														.setHashColumn(hashCol)
														.setAsLong(false)
														.build();
		return add(OperatorProto.newBuilder()
								.setAttachGeohash(attach)
								.build());
	}
	public PlanBuilder attachQuadKey(String geomCol, String srid, List<String> quadKeys,
									boolean bindOutlier, boolean bindOnce) {
		Utilities.checkNotNullArgument(geomCol, "geometry column is null");
		Utilities.checkNotNullArgument(srid, "geometry column's SRID is null");
		Utilities.checkNotNullArgument(quadKeys, "quadKeys");
		
		GeometryColumnInfoProto geomInfo = GeometryColumnInfoProto.newBuilder()
																.setName(geomCol)
																.setSrid(srid)
																.build();
		String qkSrc = "quad_keys:" + FStream.from(quadKeys).join(",");
		
		AttachQuadKeyProto attach = AttachQuadKeyProto.newBuilder()
														.setGeometryColumnInfo(geomInfo)
														.setQuadKeySource(qkSrc)
														.setBindOutlier(bindOutlier)
														.setBindOnce(bindOnce)
														.build();
		return add(OperatorProto.newBuilder()
								.setAttachQuadKey(attach)
								.build());
	}
	
	/**
	 * 입력 레코드의 두개의 좌표 컬럼 값 (x좌표 컬럼, y좌표 컬럼)을 이용하여
	 * Point 객체를 생성하여 추가한다.
	 * 만일 x좌표 컬럼 또는 y좌표 컬럼의 값이 {@code null}인 경우는 {@code null}이
	 * 결과 컬럼에 저장된다.
	 * 
	 * @param xCol		x좌표 값 컬럼 이름
	 * @param yCol		y좌표 값 컬럼 이름
	 * @param outCol	생성된 Point 객체가 저장될 컬럼 이름
	 * @return 명령이 추가된 {@code PlanBuilder} 객체.
	 */
	public PlanBuilder toPoint(String xCol, String yCol, String outCol) {
		Utilities.checkNotNullArgument(xCol, "xCol != null");
		Utilities.checkNotNullArgument(yCol, "yCol != null");
		Utilities.checkNotNullArgument(outCol, "outCol != null");
		
		ToGeometryPointProto toPoint = ToGeometryPointProto.newBuilder()
															.setXColumn(xCol)
															.setYColumn(yCol)
															.setOutColumn(outCol)
															.build();
		return add(OperatorProto.newBuilder()
								.setToPoint(toPoint)
								.build());
	}

	public PlanBuilder toXY(String geomCol, String xCol, String yCol,
										boolean keepGeomColumn) {
		Utilities.checkNotNullArgument(geomCol, "geometry column");
		Utilities.checkNotNullArgument(xCol, "x-coordinate column");
		Utilities.checkNotNullArgument(yCol, "y_coordinate column");
		
		ToXYCoordinatesProto op = ToXYCoordinatesProto.newBuilder()
														.setGeomColumn(geomCol)
														.setXColumn(xCol)
														.setYColumn(yCol)
														.setKeepGeomColumn(keepGeomColumn)
														.build();
		return add(OperatorProto.newBuilder()
								.setToXYCoordinates(op)
								.build());
	}
	public PlanBuilder toXY(String geomCol, String xCol, String yCol) {
		return toXY(geomCol, xCol, yCol, false);
	}
	
	/**
	 * 입력 레코드의 주어진 이름의 공간 컬럼 값을 그것의 무게 중심점으로 변환시키는 명령을 수행한다.
	 * 
	 * @param inGeomCol	centroid 연산 대상 공간 컬럼 이름. 
	 * @param inside	대상 영역내에서 중심점을 검색할지 여부.
	 * @param opts		연산 옵션
	 * @return 명령이 추가된 {@code PlanBuilder} 객체.
	 */
	public PlanBuilder centroid(String inGeomCol, boolean inside, GeomOpOptions opts) {
		Utilities.checkNotNullArgument(inGeomCol, "inGeomCol is null");
		Utilities.checkNotNullArgument(opts, "GeomOpOptions is null");
		
		CentroidTransformProto centroid = CentroidTransformProto.newBuilder()
																.setGeometryColumn(inGeomCol)
																.setInside(inside)
																.setOptions(opts.toProto())
																.build();
		
		return add(OperatorProto.newBuilder()
								.setCentroid(centroid)
								.build());
	}
	public PlanBuilder centroid(String inGeomCol) {
		return centroid(inGeomCol, false, GeomOpOptions.DEFAULT);
	}

	/**
	 * 입력 레코드의 주어진 이름의 공간 컬럼 값에 주어진 거리로 buffer연산을 수행한 값으로
	 * 변환시키는 명령을 수행한다.
	 * 
	 * @param geomCol	buffer 연산 대상 공간 컬럼 이름.
	 * @param distance	buffer 거리. (단위: 미터)
	 * @param opts		buffer 연산 옵션
	 * @return 명령이 추가된 {@code PlanBuilder} 객체.
	 */
	public PlanBuilder buffer(String geomCol, double distance, GeomOpOptions opts) {
		return buffer(geomCol, distance, FOption.empty(), opts);
	}
	public PlanBuilder buffer(String geomCol, double distance,
								FOption<Integer> segmentCount, GeomOpOptions opts) {
		BufferTransformProto.Builder builder = BufferTransformProto.newBuilder()
																.setGeometryColumn(geomCol)
																.setDistance(distance)
																.setOptions(opts.toProto());
		segmentCount.ifPresent(builder::setSegmentCount);
		BufferTransformProto buffer = builder.build();

		return add(OperatorProto.newBuilder()
								.setBuffer(buffer)
								.build());
	}
	public PlanBuilder buffer(String geomCol, double distance) {
		return buffer(geomCol, distance, FOption.empty(), GeomOpOptions.DEFAULT);
	}
	public PlanBuilder buffer(String geomCol, String distanceCol,
								FOption<Integer> segmentCount, GeomOpOptions opts) {
		BufferTransformProto.Builder builder = BufferTransformProto.newBuilder()
																.setGeometryColumn(geomCol)
																.setDistanceColumn(distanceCol)
																.setOptions(opts.toProto());
		segmentCount.ifPresent(builder::setSegmentCount);
		BufferTransformProto buffer = builder.build();

		return add(OperatorProto.newBuilder()
								.setBuffer(buffer)
								.build());
	}
	public PlanBuilder buffer(String geomCol, String distanceCol) {
		return buffer(geomCol, distanceCol, FOption.empty(), GeomOpOptions.DEFAULT);
	}
	public PlanBuilder buffer(String geomCol, String distanceCol, GeomOpOptions opts) {
		return buffer(geomCol, distanceCol, FOption.empty(), opts);
	}

	/**
	 * 입력 레코드의 공간 컬럼 값과 인자 {@code param}의 교집합 연산 결과로
	 * 변환시키는 명령을 수행한다.
	 * 
	 * @param geomCol	교집합 연산 대상 공간 컬럼 이름. 
	 * @param key		교집합 연산 인자.
	 * @param opts		옵션 리스트.
	 * @return 명령이 추가된 {@code PlanBuilder} 객체.
	 */
	public PlanBuilder intersection(String geomCol, Geometry key, GeomOpOptions opts) {
		Utilities.checkNotNullArgument(geomCol, "geomCol is null");
		Utilities.checkNotNullArgument(key, "key is null");
		Utilities.checkNotNullArgument(opts, "GeomOpOptions is null");

		byte[] wkb = GeoClientUtils.toWKB(key);
		UnarySpatialIntersectionProto proto
									= UnarySpatialIntersectionProto.newBuilder()
																	.setGeometryColumn(geomCol)
																	.setKey(ByteString.copyFrom(wkb))
																	.setOptions(opts.toProto())
																	.build();
		return add(OperatorProto.newBuilder()
								.setUnarySpatialIntersection(proto)
								.build());
	}
	public PlanBuilder intersection(String geomCol, Geometry key) {
		return intersection(geomCol, key, GeomOpOptions.DEFAULT);
	}
	
	/**
	 * 입력 레코드 세트에 포함된 각 레코드들에 대해 주어진 두 컬럼의 공간 객체의 교집합을 구해,
	 * 주어진 결과 컬럼에 저장하는 명령을 추가한다.
	 * 
	 * @param leftGeomCol	교집합에 사용될 첫번째 공간 객체를 포함한 컬럼 이름.
	 * @param rightGeomCol	교집합에 사용될 두번째 공간 객체를 포함한 컬럼 이름.
	 * @param outputGeomCol	계산 결과 교집합 공간 객체가 저장될 컬럼 이름과
	 * 						컬럼의 자료형을 표현한 표현식.
	 * 						일반적으로 "{@code <column_name>:<column_type>}" 형식으로 기술된다.
	 * 						예를들어 {@code "the_geom:polygon"}인 경우는 {@link DataType#POLYGON}
	 * 						형식의 "the_geom" 이라는 이름의 컬럼을 의미한다. 
	 * 						컬럼 타입이 지정하지 않은 경우는, 입력 레코드의 동일 이름의 컬럼의 동일한
	 * 						자료형을 사용하는 것을 가정한다.
	 * @param outputGeomType	출력 공간 객체 타입
	 * @return 명령이 추가된 {@code PlanBuilder} 객체.
	 */
	public PlanBuilder intersection(String leftGeomCol, String rightGeomCol,
									String outputGeomCol, DataType outputGeomType) {
		Utilities.checkNotNullArgument(leftGeomCol, "left Geometry column name");
		Utilities.checkNotNullArgument(rightGeomCol, "right Geometry column name");
		Utilities.checkNotNullArgument(outputGeomCol, "output Geometry column name");
		Utilities.checkNotNullArgument(outputGeomType, "output Geometry column type");

		TypeCodeProto outType = TypeCodeProto.valueOf(outputGeomType.getTypeCode().name());
		BinarySpatialIntersectionProto intersects = BinarySpatialIntersectionProto.newBuilder()
															.setLeftGeometryColumn(leftGeomCol)
															.setRightGeometryColumn(rightGeomCol)
															.setOutGeometryColumn(outputGeomCol)
															.setOutGeometryType(outType)
															.build();
		
		return add(OperatorProto.newBuilder()
								.setBinarySpatialIntersection(intersects)
								.build());
	}
	public PlanBuilder intersection(String leftGeomCol, String rightGeomCol,
									String outputGeomCol) {
		Utilities.checkNotNullArgument(leftGeomCol, "left Geometry column name");
		Utilities.checkNotNullArgument(rightGeomCol, "right Geometry column name");
		Utilities.checkNotNullArgument(outputGeomCol, "output Geometry column name");

		BinarySpatialIntersectionProto intersects = BinarySpatialIntersectionProto.newBuilder()
															.setLeftGeometryColumn(leftGeomCol)
															.setRightGeometryColumn(rightGeomCol)
															.setOutGeometryColumn(outputGeomCol)
															.build();
		
		return add(OperatorProto.newBuilder()
								.setBinarySpatialIntersection(intersects)
								.build());
	}

	public PlanBuilder union(String leftGeomCol, String rightGeomCol,
							String outputGeomCol, DataType outputGeomType) {
		Utilities.checkNotNullArgument(leftGeomCol, "left Geometry column name");
		Utilities.checkNotNullArgument(rightGeomCol, "right Geometry column name");
		Utilities.checkNotNullArgument(outputGeomCol, "output Geometry column name");
		Utilities.checkNotNullArgument(outputGeomType, "output Geometry column type");

		TypeCodeProto outType = PBUtils.toProto(outputGeomType.getTypeCode());
		BinarySpatialUnionProto union = BinarySpatialUnionProto.newBuilder()
															.setLeftGeometryColumn(leftGeomCol)
															.setRightGeometryColumn(rightGeomCol)
															.setOutGeometryColumn(outputGeomCol)
															.setOutGeometryType(outType)
															.build();
		
		return add(OperatorProto.newBuilder()
								.setBinarySpatialUnion(union)
								.build());
	}
	
//	/**
//	 * 입력 레코드 세트에 포함된 각 레코드들에 대해 주어진 두 컬럼의 공간 객체의 교집합을 구해,
//	 * 주어진 결과 컬럼에 저장하는 명령을 추가한다.
//	 * 
//	 * @param leftGeomCol	교집합에 사용될 첫번째 공간 객체를 포함한 컬럼 이름.
//	 * @param rightGeomCol	교집합에 사용될 두번째 공간 객체를 포함한 컬럼 이름.
//	 * @param outputGeomColSpec	계산 결과 교집합 공간 객체가 저장될 컬럼 이름과
//	 * 						컬럼의 자료형을 표현한 표현식.
//	 * 						일반적으로 "{@code <column_name>:<column_type>}" 형식으로 기술된다.
//	 * 						예를들어 {@code "the_geom:polygon"}인 경우는 {@link DataType#POLYGON}
//	 * 						형식의 "the_geom" 이라는 이름의 컬럼을 의미한다. 
//	 * 						컬럼 타입이 지정하지 않은 경우는, 입력 레코드의 동일 이름의 컬럼의 동일한
//	 * 						자료형을 사용하는 것을 가정한다.
//	 * @param precisionFactor	두 공간 객체의 교집합을 구하는 동안 오류가 발생하는 경우, 두 공간 객체의
//	 * 						정밀도를 수정한 후 다시 교집합 연산을 수행할 때 사용할 공간 좌표 정밀도를
//	 * 						소수점 자리수를 표현한다.
//	 * @return 명령이 추가된 {@code PlanBuilder} 객체.
//	 */
//	public PlanBuilder intersection(String leftGeomCol, String rightGeomCol,
//									String outputGeomColSpec, int precisionFactor);

	/**
	 * 입력 레코드 세트내 공간 객체의 정밀도를 떨어뜨리는 명령을 추가한다.
	 * 
	 * @param geomCol		정밀도 저하에 사용할 공간 객체가 기록된 컬럼 이름.
	 * @param reduceFactor	공간 좌표의 소수점 자리수.
	 * @param opts			옵션 리스트.
	 * @return 명령이 추가된 {@code PlanBuilder} 객체.
	 */
	public PlanBuilder reduceGeometryPrecision(String geomCol, int reduceFactor,
												GeomOpOptions opts) {
		Utilities.checkNotNullArgument(geomCol, "Geometry column name");
		Utilities.checkArgument(reduceFactor >= 0,
									"invalid reduceFactor: factor=" + reduceFactor);

		ReduceGeometryPrecisionProto reduce = ReduceGeometryPrecisionProto.newBuilder()
																.setGeometryColumn(geomCol)
																.setPrecisionFactor(reduceFactor)
																.setOptions(opts.toProto())
																.build();
		return add(OperatorProto.newBuilder()
								.setReducePrecision(reduce)
								.build());
	}
	
	public PlanBuilder transformCrs(String geomCol, String srcSrid, String tarSrid,
									GeomOpOptions opts) {
		TransformCrsProto proto = TransformCrsProto.newBuilder()
													.setGeometryColumn(geomCol)
													.setSourceSrid(srcSrid)
													.setTargetSrid(tarSrid)
													.setOptions(opts.toProto())
													.build();
		return add(OperatorProto.newBuilder()
								.setTransformCrs(proto)
								.build());
	}
	
	public PlanBuilder transformCrs(String geomCol, String srcSrid, String tarSrid) {
		return transformCrs(geomCol, srcSrid, tarSrid, GeomOpOptions.DEFAULT);
	}

	/**
	 * 입력 레코드의 default 공간 컬럼 값의 좌표체계를 변시키는 명령을 수행한다.
	 * <p>
	 * 좌표 체계는 EPSG코드로 기술되면 {@code "EPSG:<번호>"}와 같은 방식으로 기술된다.
	 * 예를들어 위경도 좌표계는 {@code "EPSG:4326"}으로 표현된다. 
	 * 
	 * @param srcGeomCol	좌표계 변환 대상 공간 컬럼 이름. 
	 * @param srcSrid	현재 사용하는 좌표체계
	 * @param tarSrid	변경시킬 목표 좌표체계
	 * @param tarGeomCol	좌표계 변환된 공정 정보가 저장될 컬럼 이름.
	 * @return 명령이 추가된 {@code PlanBuilder} 객체.
	 */
//	@Operator(protoId="transformCrs", name="좌표계 변경", type=OperatorType.GEO_SPATIAL)
//	public PlanBuilder transformCrs(@Parameter(protoId="geometryColumn", name="입력 공간컬럼") String srcGeomCol,
//									@Parameter(protoId="sourceSrid", name="입력 SRID") String srcSrid,
//									@Parameter(protoId="targetSrid", name="출력 SRID") String tarSrid,
//									@Parameter(protoId="options/outGeomCol", name="출력 공간컬럼") String tarGeomCol) {
//		return transformCrs(srcGeomCol, srcSrid, tarSrid, OUTPUT(tarGeomCol));
//	}
	
	//***********************************************************************
	//***********************************************************************
	//	공간 정보 조인 연산자들
	//***********************************************************************
	//***********************************************************************

	/**
	 * 주어진 두 데이터세트의 default 공간 컬럼 값을 기반으로 주어진 공간 관계를 만족하는 레코드 쌍으로
	 * 구성된 레코드를 생성하는 조인을 수행한다.
	 * <p>
	 * 입력으로 사용되는 두 데이터세트 모두 clustered 형식이어야 한다.
	 * 조인 결과 레코드는 양쪽 데이터세트에 속한 레코드들 중에서 {@code outputColumns} 인자에 기술된
	 * 컬럼만으로 구성된다.왼쪽 데이터세트에 포함된 컬럼들을 지정할 때는 'left' namespace를 사용해야 하고,
	 * 오른쪽 데이터세트에 포함된 컬럼들을 지정할 때는 'right' namespace를 사용해야 한다.
	 * 예를 들어 {@code "left.the_geom,rigfht.id"}인 경우는 결과 레코등에 왼쪽 데이터세트의
	 * 'the_geom' 컬럼 값과 오른쪽 데이터세트의 'id'로 구성된 결과 레코드가 생성된다.
	 * 
	 * @param leftDataSet	조인에 가담할 기본 데이터세트 이름. 
	 * @param rightDataSet	조인에 가담할 인자 데이터세트 이름.
	 * @param opts			조인 옵션 리스트
	 * @return 명령이 추가된 {@code PlanBuilder} 객체.
	 * 
	 * @return 명령이 추가된 {@code PlanBuilder} 객체. 
	 */
	public PlanBuilder loadSpatialIndexJoin(String leftDataSet, String rightDataSet,
											SpatialJoinOptions opts) {
		Utilities.checkNotNullArgument(leftDataSet, "leftDataSet is null");
		Utilities.checkNotNullArgument(rightDataSet, "rightDataSet is null");
		Utilities.checkNotNullArgument(opts, "joinExpr is null");
		
		LoadSpatialIndexJoinProto op = LoadSpatialIndexJoinProto.newBuilder()
																.setLeftDataset(leftDataSet)
																.setRightDataset(rightDataSet)
																.setOptions(opts.toProto())
																.build();
		return add(OperatorProto.newBuilder()
								.setLoadSpatialIndexJoin(op)
								.build());
	}
	
	public PlanBuilder loadSpatialIndexJoin(String leftDataSet, String rightDataSet,
											String outCols) {
		return loadSpatialIndexJoin(leftDataSet, rightDataSet,
									SpatialJoinOptions.OUTPUT(outCols));
	}

	/**
	 * 입력 레코드들과 인자로 주이진 데이터세트에 속한 레코드들 사이에 조인을 수행한다.
	 * <p>
	 * 인자로 주어진 데이터세트는 사전에 공간 인덱스가 존재해야 한다.
	 * 사용되는 조인 관계식은 기본적으로 {@link SpatialRelation#INTERSECTS}을 사용하며,
	 * 변경할 경우에는 {@link SpatialJoinOptions}을 사용한다.
	 * 조인 결과는 입력 레코드 세트에 포함된 모든 컬럼과 인자 데이터세트의 모든 컬럼들로
	 * 구성되며, 컬럼 이름 충돌 방지를 위해 인자 데이터세트의 모든 컬럼 이름에는 'param_' prefix가
	 * 붙는다.
	 * 
	 * @param geomCol	입력 레코드 조인 컬럼.
	 * @param paramDataSet	내부 조인 데이터세트 이름.
	 * @param opts		조인 관련 옵션 리스트
	 * 
	 * @return 명령이 추가된 {@code PlanBuilder} 객체. 
	 */
	public PlanBuilder spatialJoin(String geomCol, String paramDataSet, SpatialJoinOptions opts) {
		Utilities.checkNotNullArgument(geomCol, "input Geometry column is null");
		Utilities.checkNotNullArgument(paramDataSet, "parameter DataSet id is null");
		
		SpatialBlockJoinProto join = SpatialBlockJoinProto.newBuilder()
														.setGeomColumn(geomCol)
														.setParamDataset(paramDataSet)
														.setOptions(opts.toProto())
														.build();
		return add(OperatorProto.newBuilder()
								.setSpatialBlockJoin(join)
								.build());
	}

	/**
	 * 입력 레코드들과 인자로 주이진 데이터세트에 속한 레코드들 사이에 조인을 수행한다.
	 * <p>
	 * 인자로 주어진 데이터세트는 사전에 공간 인덱스가 존재해야 한다.
	 * 사용되는 조인 관계식은 기본적으로 {@link SpatialRelation#INTERSECTS}을 사용하며,
	 * 변경할 경우에는 {@link SpatialJoinOptions}을 사용한다.
	 * 조인 결과는 입력 레코드 세트에 포함된 모든 컬럼과 인자 데이터세트의 모든 컬럼들로
	 * 구성되며, 컬럼 이름 충돌 방지를 위해 인자 데이터세트의 모든 컬럼 이름에는 'param_' prefix가
	 * 붙는다.
	 * 
	 * @param geomCol	입력 레코드 조인 컬럼.
	 * @param paramDataSet	내부 조인 데이터세트 이름.
	 * @param outputColumns 조인 결과 컬럼 리스트.
	 * @param opts		조인 관련 옵션 리스트
	 * 
	 * @return 명령이 추가된 {@code PlanBuilder} 객체. 
	 */
	public PlanBuilder spatialJoin(String geomCol, String paramDataSet,
									String outputColumns, SpatialJoinOptions opts) {
		Utilities.checkNotNullArgument(outputColumns, "output column expression is null");
		
		return spatialJoin(geomCol, paramDataSet, opts.outputColumns(outputColumns));
	}
	public PlanBuilder spatialJoin(String geomCol, String paramDataSet, String outputColumns) {
		return spatialJoin(geomCol, paramDataSet, outputColumns, SpatialJoinOptions.EMPTY);
	}
	
	public PlanBuilder spatialOuterJoin(String inputGeomCol, String paramDataSet,
										SpatialJoinOptions opts) {
		Utilities.checkNotNullArgument(inputGeomCol, "input Geometry column is null");
		Utilities.checkNotNullArgument(paramDataSet, "parameter DataSet id is null");

		SpatialOuterJoinProto join = SpatialOuterJoinProto.newBuilder()
															.setGeomColumn(inputGeomCol)
															.setParamDataset(paramDataSet)
															.setOptions(opts.toProto())
															.build();
		return add(OperatorProto.newBuilder()
								.setSpatialOuterJoin(join)
								.build());
	}
	
	public PlanBuilder spatialOuterJoin(String geomCol, String paramDataSet,
										String outputColumns) {
		Utilities.checkNotNullArgument(outputColumns, "output columns are null");
		
		return spatialOuterJoin(geomCol, paramDataSet, SpatialJoinOptions.OUTPUT(outputColumns));
	}
	
	public PlanBuilder spatialSemiJoin(String geomCol, String paramDataSet,
										SpatialJoinOptions opts) {
		Utilities.checkNotNullArgument(geomCol, "input Geometry column is null");
		Utilities.checkNotNullArgument(paramDataSet, "parameter DataSet id is null");

		SpatialSemiJoinProto join = SpatialSemiJoinProto.newBuilder()
														.setGeomColumn(geomCol)
														.setParamDataset(paramDataSet)
														.setOptions(opts.toProto())
														.build();
		return add(OperatorProto.newBuilder()
								.setSpatialSemiJoin(join)
								.build());
	}
	
	public PlanBuilder spatialSemiJoin(String geomCol, String paramDataSet) {
		return spatialSemiJoin(geomCol, paramDataSet, SpatialJoinOptions.EMPTY);
	}
	
	/**
	 * 거리 기반 공간 조인 연산을 추가한다.
	 * 
	 * @param geomCol	공간 조인 대상 입력 레코드 냉 공간 컬럼 이름.
	 * @param paramDataSet	공간 조인 대상 인자 DataSet 식별자.
	 * @param topK			매칭되는 최대 조인쌍 갯수. 매칭되는 레코드 쌍의 수가 topK보다 큰 경우는
	 * 						두 레코드의 거리 값을 기준을 가까운 순서대로 topK 갯수만큼만 선택됨.
	 * 						음수인 경우는 매칭되는 모든 레코드쌍을 선택하는 것의 의미.
	 * @param dist			조인 매칭 최대 거리.
	 * @param opts			옵션 리스트.
	 * @return 명령이 추가된 {@code PlanBuilder} 객체. 
	 */
	public PlanBuilder knnJoin(String geomCol, String paramDataSet, int topK, double dist,
								SpatialJoinOptions opts) {
		Utilities.checkNotNullArgument(geomCol, "input Geometry column is null");
		Utilities.checkNotNullArgument(paramDataSet, "parameter DataSet id is null");
		Utilities.checkArgument(topK > 0, "invalid top-k: " + topK);
		Utilities.checkArgument(Double.compare(dist, 0) > 0, "invalid distance: " + dist);
		
		SpatialKnnInnerJoinProto join = SpatialKnnInnerJoinProto.newBuilder()
																.setGeomColumn(geomCol)
																.setParamDataset(paramDataSet)
																.setTopK(topK)
																.setRadius(dist)
																.setOptions(opts.toProto())
																.build();
		return add(OperatorProto.newBuilder()
								.setSpatialKnnJoin(join)
								.build());
	}
	public PlanBuilder knnJoin(String geomCol, String paramDsId, int topK, double dist,
								String outCols) {
		return knnJoin(geomCol, paramDsId, topK, dist,
						SpatialJoinOptions.OUTPUT(outCols));
	}

	public PlanBuilder knnOuterJoin(String inputGeomCol, String paramDataSet, int topK,
									double dist, SpatialJoinOptions opts) {
		Utilities.checkNotNullArgument(inputGeomCol, "input Geometry column is null");
		Utilities.checkNotNullArgument(paramDataSet, "parameter DataSet id is null");
		Utilities.checkArgument(topK > 0, "invalid top-k: " + topK);
		Utilities.checkArgument(Double.compare(dist, 0) > 0, "invalid distance: " + dist);
		
		SpatialKnnOuterJoinProto join = SpatialKnnOuterJoinProto.newBuilder()
																.setGeomColumn(inputGeomCol)
																.setParamDataset(paramDataSet)
																.setTopK(topK)
																.setRadius(dist)
																.setOptions(opts.toProto())
																.build();
		return add(OperatorProto.newBuilder()
								.setSpatialKnnOuterJoin(join)
								.build());
	}
	public PlanBuilder knnOuterJoin(String inputGeomCol, String paramDataSet, int topK,
									double dist, String outCols) {
		return knnOuterJoin(inputGeomCol, paramDataSet, topK, dist,
							SpatialJoinOptions.OUTPUT(outCols));
	}

	/**
	 * 입력 레코드들과 인자로 주이진 데이터세트에 속한 레코드들 사이에 clip 조인을 수행한다.
	 * <p>
	 * 인자로 주어진 데이터세트는 clustered 형식이어야 한다.
	 * 조인 방법은 중첩루프(nested-loop) 방식으로 각 입력 레코드에 대해 겹치는
	 * 모든 {@code paramLayer} 레코드들을 검색하여 clip 작업을 수행하고, 그 결과가
	 * 출력 레코드의 공간 컬럼에 포함된다.
	 * 출력 레코드는 입력 레코드에 포함된 컬럼으로만 구성되고, 공간 컬럼만이 clip 작업 결과로
	 * 대치된다.
	 * 
	 * @param inputGeomCol	입력 레코드 중에서 조인에 사용할 공간 객체 컬럼 이름.
	 * @param clipDataSet	클립 조인 인자 데이터세트 이름.
	 * 
	 * @return 명령이 추가된 {@code PlanBuilder} 객체. 
	 */
	public PlanBuilder arcClip(String inputGeomCol, String clipDataSet) {
		Utilities.checkNotNullArgument(inputGeomCol, "input Geometry column is null");
		Utilities.checkNotNullArgument(clipDataSet, "Clip DataSet id is null");
		
		ArcClipProto clip = ArcClipProto.newBuilder()
										.setGeomColumn(inputGeomCol)
										.setParamDataset(clipDataSet)
										.build();
		return add(OperatorProto.newBuilder()
								.setArcClip(clip)
								.build());
	}
	
	public PlanBuilder intersectionJoin(String geomCol, String paramDataSet,
										SpatialJoinOptions opts) {
		Utilities.checkNotNullArgument(geomCol, "input Geometry column is null");
		Utilities.checkNotNullArgument(paramDataSet, "parameter DataSet id is null");

		SpatialIntersectionJoinProto join = SpatialIntersectionJoinProto.newBuilder()
																	.setGeomColumn(geomCol)
																	.setParamDataset(paramDataSet)
																	.setOptions(opts.toProto())
																	.build();
		return add(OperatorProto.newBuilder()
								.setSpatialIntersectionJoin(join)
								.build());
	}
	
	/**
	 * 입력 레코드들과 인자로 주이진 데이터세트에 속한 레코드들 사이에 difference 조인을 수행한다.
	 * <p>
	 * 인자로 주어진 데이터세트는 clustered 형식이어야 한다.
	 * 조인 방법은 중첩루프(nested-loop) 방식으로 각 입력 레코드에 대해 이것과 겹치는
	 * 모든 {@code paramLayerName} 레코드들을 검색하여 이 레코드들과 difference 작업을
	 * 수행하여 결과를 출력 레코드에 포함시킨다.
	 * 출력 레코드는 입력 레코드에 포함된 컬럼으로만 구성되고, 공간 컬럼만이 clip 작업 결과로
	 * 대치된다.
	 * 
	 * @param inputGeomCol 공간 정보 컬럼 이름
	 * @param paramDataSet	Difference 조인 인자 데이터세트 이름.
	 * @return 명령이 추가된 {@code PlanBuilder} 객체. 
	 */
	public PlanBuilder differenceJoin(String inputGeomCol, String paramDataSet) {
		Utilities.checkNotNullArgument(inputGeomCol, "input Geometry column is null");
		Utilities.checkNotNullArgument(paramDataSet, "clipper DataSet id is null");
		
		SpatialDifferenceJoinProto clip = SpatialDifferenceJoinProto.newBuilder()
													.setGeomColumn(inputGeomCol)
													.setParamDataset(paramDataSet)
													.build();
		return add(OperatorProto.newBuilder()
								.setSpatialDifferenceJoin(clip)
								.build());
	}
	
	public PlanBuilder spatialAggregateJoin(String geomCol, String paramDataSet,
											AggregateFunction... aggrFuncs) {
		return spatialAggregateJoin(geomCol, paramDataSet, aggrFuncs, SpatialJoinOptions.EMPTY);
	}
	
	public PlanBuilder spatialAggregateJoin(String inputGeomCol, String paramDataSet,
											AggregateFunction[] aggrFuncs,
											SpatialJoinOptions opts) {
		Utilities.checkNotNullArgument(inputGeomCol, "input Geometry column");
		Utilities.checkNotNullArgument(paramDataSet, "parameter DataSet id");
		Utilities.checkArgument(aggrFuncs != null && aggrFuncs.length > 0,
									"empty AggregateFunction list");
		
		ValueAggregateReducersProto reducer = FStream.of(aggrFuncs)
												.map(AggregateFunction::toProto)
												.foldLeft(ValueAggregateReducersProto.newBuilder(),
															(b,a) -> b.addAggregate(a))
												.build();
		SpatialReduceJoinProto join = SpatialReduceJoinProto.newBuilder()
															.setGeomColumn(inputGeomCol)
															.setParamDataset(paramDataSet)
															.setReducer(reducer)
															.setOptions(opts.toProto())
															.build();
		return add(OperatorProto.newBuilder()
								.setSpatialReduceJoin(join)
								.build());
	}

	public PlanBuilder arcSpatialJoin(String inputGeomCol, String paramDataSet,
										boolean oneToMany, boolean includeParamCols,
										FOption<String> joinExpr) {
		Utilities.checkNotNullArgument(inputGeomCol, "input Geometry column is null");
		Utilities.checkNotNullArgument(paramDataSet, "parameter DataSet id is null");
		
		ArcSpatialJoinProto.Builder builder
							= ArcSpatialJoinProto.newBuilder()
												.setGeomColumn(inputGeomCol)
												.setParamDataset(paramDataSet)
												.setOneToMany(oneToMany)
												.setIncludeParamCols(includeParamCols);
		joinExpr.ifPresent(builder::setJoinExpr);
		ArcSpatialJoinProto proto = builder.build();
		
		return add(OperatorProto.newBuilder()
								.setArcSpatialJoin(proto)
								.build());
	}
	public PlanBuilder arcSpatialJoin(String inputGeomCol, String paramDataSet,
										boolean oneToMany, boolean includeParamCols) {
		return arcSpatialJoin(inputGeomCol, paramDataSet, oneToMany, includeParamCols,
								FOption.empty());
	}
	
	public PlanBuilder interpolateSpatially(String geomColumn, String paramDataSet,
											String valueColumns, double radius,
											String outputColumns, InterpolationMethod method) {
		Utilities.checkNotNullArgument(geomColumn, "input Geometry column");
		Utilities.checkNotNullArgument(paramDataSet, "paramDataSet");
		Utilities.checkNotNullArgument(valueColumns, "value columns");
		Utilities.checkNotNullArgument(outputColumns, "output columns");
		Utilities.checkNotNullArgument(method, "interpolation method");
		
		InterpolateSpatiallyProto op = InterpolateSpatiallyProto.newBuilder()
													.setGeomColumn(geomColumn)
													.setParamDataset(paramDataSet)
													.setValueColumns(valueColumns)
													.setOutputColumns(outputColumns)
													.setRadius(radius)
													.setInterpolationMethod(method.toStringExpr())
													.build();
		
		return add(OperatorProto.newBuilder()
								.setSpatialInterpolation(op)
								.build());
	}
	
	public PlanBuilder interpolateSpatially(String geomColumn, String paramDataSet,
											String valueColumns, double radius, int topK,
											String outputColumns, InterpolationMethod method) {
		Utilities.checkNotNullArgument(geomColumn, "input Geometry column");
		Utilities.checkNotNullArgument(paramDataSet, "paramDataSet");
		Utilities.checkNotNullArgument(valueColumns, "value columns");
		Utilities.checkNotNullArgument(outputColumns, "output columns");
		Utilities.checkNotNullArgument(method, "interpolation method");
		Utilities.checkArgument(topK > 0, "invalid top-k");
		
		InterpolateSpatiallyProto op = InterpolateSpatiallyProto.newBuilder()
													.setGeomColumn(geomColumn)
													.setParamDataset(paramDataSet)
													.setValueColumns(valueColumns)
													.setOutputColumns(outputColumns)
													.setRadius(radius)
													.setTopK(topK)
													.setInterpolationMethod(method.toStringExpr())
													.build();
		
		return add(OperatorProto.newBuilder()
								.setSpatialInterpolation(op)
								.build());
	}
	
	public PlanBuilder estimateIdw(String geomColumn, String paramDataSet, String valueColumn,
									double radius, int topK, String densityColumn,
									FOption<Double> power) {
		Utilities.checkNotNullArgument(geomColumn, "input Geometry column is null");
		Utilities.checkNotNullArgument(paramDataSet, "paramDataSet is null");
		Utilities.checkNotNullArgument(densityColumn, "densityColumn is null");
		Utilities.checkArgument(Double.compare(radius, 0) > 0,
									"invalid radius: radius=" + radius);
		
		EstimateIDWProto.Builder builder = EstimateIDWProto.newBuilder()
														.setGeomColumn(geomColumn)
														.setParamDataset(paramDataSet)
														.setValueColumn(valueColumn)
														.setOutputDensityColumn(densityColumn)
														.setRadius(radius)
														.setTopK(topK);
		power.ifPresent(builder::setPower);
		EstimateIDWProto estimate = builder.build();
		
		return add(OperatorProto.newBuilder()
								.setEstimateIdw(estimate)
								.build());
	}
	
	public PlanBuilder estimateKernelDensity(String geomColumn, String dataset, String valueColumn,
											double radius, String densityColumn) {
		Utilities.checkNotNullArgument(geomColumn, "input Geometry column is null");
		Utilities.checkNotNullArgument(dataset, "dataset is null");
		Utilities.checkNotNullArgument(densityColumn, "densityColumn is null");
		Utilities.checkArgument(Double.compare(radius, 0) > 0,
									"invalid radius: radius=" + radius);
		
		EstimateKernelDensityProto estimate = EstimateKernelDensityProto.newBuilder()
																.setGeomColumn(geomColumn)
																.setDataset(dataset)
																.setValueColumn(valueColumn)
																.setDensityColumn(densityColumn)
																.setRadius(radius)
																.build();
		
		return add(OperatorProto.newBuilder()
								.setEstimateKernelDensity(estimate)
								.build());
	}

	public PlanBuilder loadLocalMoranI(String dataset, String idColumn,
										String valueColumn, double radius, LISAWeight weight) {
		Utilities.checkNotNullArgument(dataset, "dataset is null");
		Utilities.checkNotNullArgument(idColumn, "idColumn is null");
		Utilities.checkNotNullArgument(valueColumn, "valueColumn is null");
		Utilities.checkArgument(Double.compare(radius, 0) > 0, "invalid radius: " + radius);
		Utilities.checkNotNullArgument(weight, "weight is null");
		
		LoadLocalMoransIProto load = LoadLocalMoransIProto.newBuilder()
											.setDataset(dataset)
											.setIdColumn(idColumn)
											.setValueColumn(valueColumn)
											.setRadius(radius)
											.setWeightType(LISAWeightProto.valueOf(weight.name()))
											.build();

		return add(OperatorProto.newBuilder()
								.setLoadLocalMoransI(load)
								.build());
	}
	public PlanBuilder loadGetisOrdGi(String dataset, String valueColumn,
											double radius, LISAWeight weight) {
		Utilities.checkNotNullArgument(dataset, "dataset is null");
		Utilities.checkNotNullArgument(valueColumn, "valueColumn is null");
		Utilities.checkArgument(Double.compare(radius, 0) > 0, "invalid radius: " + radius);
		Utilities.checkNotNullArgument(weight, "weight is null");
		
		LoadGetisOrdGiProto load = LoadGetisOrdGiProto.newBuilder()
											.setDataset(dataset)
											.setValueColumn(valueColumn)
											.setRadius(radius)
											.setWeightType(LISAWeightProto.valueOf(weight.name()))
											.build();

		return add(OperatorProto.newBuilder()
								.setLoadGetisOrdGi(load)
								.build());
	}
	
//	//***********************************************************************
//	//***********************************************************************
//	//	기타 공간 정보 관련 연산자들
//	//***********************************************************************
//	//***********************************************************************

	/**
	 * 입력 레코드들을 주어진 그룹핑 컬럼을 기준으로 dissolve 연산을 수행하는 명령을 수행한다.
	 * 
	 * @param keyCols	그룹핑 기준 컬럼 리스트. 
	 * @param geomCol	dissolve할 대상 공간 컬럼.
	 * @return 명령이 추가된 {@code PlanBuilder} 객체. 
	 */
	public PlanBuilder dissolve(String keyCols, String geomCol) {
		Utilities.checkNotNullArgument(keyCols, "keyCols is null");
		Utilities.checkNotNullArgument(geomCol, "geomCol is null");
		
		DissolveProto dissolve = DissolveProto.newBuilder()
											.setKeyColumns(keyCols)
											.setGeometryColumn(geomCol)
											.build();

		return add(OperatorProto.newBuilder()
								.setDissolve(dissolve)
								.build());
	}
	
	public PlanBuilder flattenGeometry(String geomCol, DataType outGeomType) {
		Utilities.checkNotNullArgument(geomCol, "geomCol");

		TypeCodeProto tc = TypeCodeProto.valueOf(outGeomType.getTypeCode().name());
		FlattenGeometryProto flatten = FlattenGeometryProto.newBuilder()
															.setGeometryColumn(geomCol)
															.setOutGeometryType(tc)
															.build();

		return add(OperatorProto.newBuilder()
								.setFlattenGeometry(flatten)
								.build());
	}
	
	public PlanBuilder validateGeometry(String geomCol) {
		Utilities.checkNotNullArgument(geomCol, "geomCol is null");

		ValidateGeometryProto validate = ValidateGeometryProto.newBuilder()
														.setGeometryColumn(geomCol)
														.build();

		return add(OperatorProto.newBuilder()
								.setValidateGeometry(validate)
								.build());
	}
	
	public PlanBuilder breakLineString(String geomCol) {
		Utilities.checkNotNullArgument(geomCol, "geomCol is null");

		BreakLineStringProto breakLine = BreakLineStringProto.newBuilder()
														.setGeometryColumn(geomCol)
														.build();

		return add(OperatorProto.newBuilder()
								.setBreakLine(breakLine)
								.build());
	}
	
	public PlanBuilder splitGeometry(String geomCol) {
		Utilities.checkNotNullArgument(geomCol, "geomCol is null");
		
		SplitGeometryProto dissolve = SplitGeometryProto.newBuilder()
														.setGeometryColumn(geomCol)
														.build();

		return add(OperatorProto.newBuilder()
								.setSplitGeometry(dissolve)
								.build());
	}
}
