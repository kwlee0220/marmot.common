package marmot.dataset;

import com.vividsolutions.jts.geom.Envelope;

import marmot.Plan;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.geo.catalog.IndexNotFoundException;
import marmot.geo.catalog.SpatialIndexInfo;
import marmot.geo.command.CreateSpatialIndexOptions;
import marmot.geo.query.RangeQueryEstimate;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface DataSet {
	/**
	 * 데이터세트의 식별자(이름)을 반환한다.
	 * 
	 * @return 식별자.	
	 */
	public String getId();
	
	/**
	 * 데이터세트의 레코드 스키마 ({@link RecordSchema})를 반환한다.
	 * 
	 * @return 레코드 스키마.	
	 */
	public RecordSchema getRecordSchema();
	
	/**
	 * 데이터세트의 타입을 반환한다.
	 * 
	 * @return 타입.	
	 */
	public DataSetType getType();
	
	/**
	 * 데이터세트의 디렉토리 이름을 반환한다.
	 * 
	 * @return	디렉토리 이름
	 */
	public String getDirName();
	
	/**
	 * 기본 공간 컬럼 지정 여부를 반환한다.
	 * 
	 * @return	공간 컬럼 지정 여부.
	 */
	public boolean hasGeometryColumn();

	/**
	 * 데이터세트에 정의된 기본 공간 컬럼의 정보를 반환한다.
	 * <p>
	 * 공간 컬럼이 정의되지 않은 경우는 {@link GeometryColumnNotExistsException} 예외를 발생시킨다.
	 * 
	 * @return	공간 컬럼 정보
	 * @throws	GeometryColumnNotExistsException 공간 컬럼이 존재하지 않은 경우.
	 */
	public GeometryColumnInfo getGeometryColumnInfo();

	/**
	 * 데이터세트에 정의된 기본 공간 컬럼의 이름을 반환한다.
	 * <p>
	 * 공간 컬럼이 정의되지 않은 경우는 {@link GeometryColumnNotExistsException} 예외를 발생시킨다.
	 * 
	 * @return	공간 컬럼 이름
	 * @throws	GeometryColumnNotExistsException 공간 컬럼이 존재하지 않은 경우.
	 */
	public default String getGeometryColumn() {
		return getGeometryColumnInfo().name();
	}
	
	/**
	 * 데이터세트에 정의된 기본 공간 컬럼의 순번을 반환한다.
	 * <p>
	 * 공간 컬럼이 정의되지 않은 경우는 {@link GeometryColumnNotExistsException} 예외를 발생시킨다.
	 * 
	 * @return	공간 컬럼 순번
	 * @throws	GeometryColumnNotExistsException 공간 컬럼이 존재하지 않은 경우.
	 */
	public default int getGeometryColumnIndex() {
		return getRecordSchema().getColumn(getGeometryColumnInfo().name()).ordinal();
	}
	
	/**
	 * 모든 레코드의 기본 공간 컬럼에 기록된 공간 정보의 MBR을 반환한다.
	 * <p>
	 * 공간 컬럼이 정의되지 않은 경우는 {@link GeometryColumnNotExistsException} 예외를 발생시킨다.
	 * 
	 * @return	MBR 좌표 또는 {@code null}
	 * @throws	GeometryColumnNotExistsException 공간 컬럼이 존재하지 않은 경우.
	 */
	public Envelope getBounds();
	
	/**
	 * 데이터세트에 포함된 모든 레코드의 갯수를 반환한다.
	 * 
	 * @return	레코드의 갯수
	 */
	public long getRecordCount();
	
	/**
	 * 데이터세트가 저장된 HDFS내의 파일 경로명을 반환한다.
	 * 
	 * @return	파일 경로명
	 */
	public String getHdfsPath();
	
	/**
	 * 테이터 세트가 저장된 HDFS 파일의 블럭 크기를 반환한다.
	 * 
	 * @return	파일 블럭 크기 (바이트 단위)
	 */
	public long getBlockSize();
	
	/**
	 * 데이터 세트의 압축여부를 반환한다.
	 * 
	 * @return	반환 여부
	 */
	public FOption<String> getCompressionCodecName();
	
	/**
	 * 본 데이터 세트가 공간 클러스터가 존재하는지 유무를 반환한다.
	 * 
	 * @return 공간 클러스터 존재 유무
	 */
	public default boolean isSpatiallyClustered() {
		return getDefaultSpatialIndexInfo().isPresent();
	}
	
	/**
	 * 기본 공간 컬럼에 부여된 인덱스 등록정보를 반환한다.
	 * <p>
	 * 공간 인덱스가 생성되어 있지 않은 경우는 {@link FOption#empty()}가 반환된다.
	 * 
	 * @return	공간 인덱스 등록정보.
	 */
	public FOption<SpatialIndexInfo> getDefaultSpatialIndexInfo();

	/**
	 * 데이터세트의 크기를 바이트 단위로 반환한다. 
	 * 
	 * @return	 데이터세트 길이.
	 */
	public long length();
	
	/**
	 * 기본 공간 컬럼 정보를 설정한다.
	 * <p>
	 * 기존 공간 컬럼 정보를 제거하려는 경우는 {@link FOption#empty()} 를 사용한다.
	 * 
	 * @param gcInfo	공간 컬럼 정보.
	 * @return	공간 컬럼 정보가 갱신된 데이터 세트
	 */
	public DataSet updateGeometryColumnInfo(FOption<GeometryColumnInfo> gcInfo);
	
	/**
	 * 데이터세트에 저장된 레코드 세트를 읽는다.
	 * 
	 * @return	데이터세트에 기록된 레코드 세트. 
	 */
	public RecordSet read();
	
	/**
	 * 주어진 레코드 세트를 데이터세트에 추가한다.
	 * <p>
	 * 본 데이터세트의 스키마와 인자로 주어진 레코드세트의 스키마는 동일해야 한다.
	 * 
	 * @param rset	저장할 레코드 세트.
	 * @return	본 데이터 세트 객체.
	 */
	public long append(RecordSet rset);
	
	/**
	 * 주어진 레코드 세트에 Plan을 적용시킨 결과를 데이터세트에 추가한다.
	 * <p>
	 * 본 데이터세트의 스키마와 인자로 주어진 Plan의 수행결과로 생성되는 레코드세트의 스키마는 동일해야 한다.
	 * @param rset	Plan 수행시 사용할 입력 레코드세트.
	 * @param plan	저장시킬 레코드를 생성할 Plan 객체.
	 * 
	 * @return	본 데이터 세트 객체.
	 */
	public long append(RecordSet rset, Plan plan);
	
	/**
	 * 데이터세트의 기본 공간 컬럼을 기준으로 인덱스(클러스터)를 생성한다.
	 * <p>
	 * 공간 인덱스가 생성되어 있지 않은 경우는 오류가 발생된다.
	 * 
	 * @return	생성된 인덱스의 등록정보.
	 */
	public default SpatialIndexInfo cluster() {
		return cluster(CreateSpatialIndexOptions.DEFAULT());
	}
	
	/**
	 * 데이터세트의 기본 공간 컬럼을 기준으로 인덱스(클러스터)를 생성한다.
	 * <p>
	 * 공간 인덱스가 생성되어 있지 않은 경우는 오류가 발생된다.
	 * 
	 * @param opts	공간 인덱스 생성 관련 인자.
	 * @return	생성된 인덱스의 등록정보.
	 */
	public SpatialIndexInfo cluster(CreateSpatialIndexOptions opts);
	
	/**
	 * 본 데이터 세트에 생성된 공간 인덱스(클러스터)를 삭제한다.
	 */
	public void deleteSpatialCluster();
	
	/**
	 * 본 데이터 세트의 공간 색인 영역 중에서 주어진 질의 영역과 겹치는 레코드들의 수와
	 * 공간 파티션별 레코드 수를 추정치를 반환한다.
	 * 
	 *  @param range	질의 영역
	 *  @return	질의 결과 추정치
	 */
	public RangeQueryEstimate estimateRangeQuery(Envelope range);
	
	/**
	 * 본 데이터 세트의 공간 색인 영역 중에서 주어진 질의 영역과 겹치는 레코드들 중에서
	 * 무작위로 주어진 샘플수 만큼 선택해서 반환한다.
	 * 
	 * @param range		질의 영역
	 * @param nsample	샘플수
	 */
	public RecordSet queryRange(Envelope range, int nsamples);
	
	/**
	 * 주어진 공간 파티션 식별자에 해당하는 파티션에 저장된 모든 공간 데이터를 반환한다.
	 * 
	 *  @param quadKey	공간 파티션 식별자.
	 *  @return	공간 데이터 집합
	 */
	public RecordSet readSpatialCluster(String quadKey);
	
	/**
	 * Thumbnail을 생성한다.
	 * 
	 * @param	샘플 수
	 * @throws	IndexNotFoundException	공간 색인이 존재하지 않는 경우.
	 */
	public void createThumbnail(int sampleCount) throws IndexNotFoundException;
	
	/**
	 * Thumbnail을 제거한다.
	 */
	public boolean deleteThumbnail();
	
	public default boolean hasThumbnail() {
		return getThumbnailRatio().isPresent();
	}
	
	public FOption<Float> getThumbnailRatio();
}
