package marmot.geo.command;

import java.util.List;

import com.google.common.collect.Lists;
import com.vividsolutions.jts.geom.Envelope;

import marmot.proto.service.EstimateQuadKeysOptionsProto;
import marmot.protobuf.PBUtils;
import marmot.support.PBSerializable;
import utils.UnitUtils;
import utils.Utilities;
import utils.func.FOption;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class EstimateQuadKeysOptions implements PBSerializable<EstimateQuadKeysOptionsProto> {
	private static final EstimateQuadKeysOptions DEFAULT
				= new EstimateQuadKeysOptions(FOption.empty(), FOption.empty(), FOption.empty(),
												FOption.empty(), FOption.empty());

	private final FOption<Integer> m_mapperCount;
	private final FOption<Envelope> m_validRange;
	private final FOption<Long> m_sampleSize;
	private final FOption<Integer> m_maxQuadKeyLength;
	private final FOption<Long> m_clusterSize;
	
	private EstimateQuadKeysOptions(FOption<Integer> mapperCount, FOption<Envelope> validRange,
									FOption<Long> sampleSize, FOption<Integer> maxQuadKeyLength,
									FOption<Long> clusterSize) {
		m_mapperCount = mapperCount;
		m_validRange = validRange;
		m_sampleSize = sampleSize;
		m_maxQuadKeyLength = maxQuadKeyLength;
		m_clusterSize = clusterSize;
	}
	
	public static EstimateQuadKeysOptions DEFAULT() {
		return DEFAULT;
	}
	
	public static EstimateQuadKeysOptions SAMPLE_SIZE(long size) {
		return DEFAULT.sampleSize(size);
	}
	
	public FOption<Integer> mapperCount() {
		return m_mapperCount;
	}
	public EstimateQuadKeysOptions mapperCount(FOption<Integer> mapperCount) {
		return new EstimateQuadKeysOptions(mapperCount, m_validRange, m_sampleSize,
											m_maxQuadKeyLength, m_clusterSize);
	}
	public EstimateQuadKeysOptions mapperCount(int mapperCount) {
		return mapperCount(FOption.of(mapperCount));
	}
	
	public FOption<Envelope> validRange() {
		return m_validRange;
	}
	public EstimateQuadKeysOptions validRange(FOption<Envelope> validRange) {
		return new EstimateQuadKeysOptions(m_mapperCount, validRange, m_sampleSize,
											m_maxQuadKeyLength, m_clusterSize);
	}
	public EstimateQuadKeysOptions validRange(Envelope validRange) {
		Utilities.checkNotNullArgument(validRange, "valid_range");
		
		return validRange(FOption.of(validRange));
	}
	
	public FOption<Long> sampleSize() {
		return m_sampleSize;
	}
	public EstimateQuadKeysOptions sampleSize(FOption<Long> sampleSize) {
		return new EstimateQuadKeysOptions(m_mapperCount, m_validRange, sampleSize,
											m_maxQuadKeyLength, m_clusterSize);
	}
	public EstimateQuadKeysOptions sampleSize(long sampleSize) {
		Utilities.checkArgument(sampleSize > 0, "invalid sampleSize=" + sampleSize);
		
		return sampleSize(FOption.of(sampleSize));
	}
	
	public FOption<Integer> maxQuadKeyLength() {
		return m_maxQuadKeyLength;
	}
	public EstimateQuadKeysOptions maxQuadKeyLength(int length) {
		Utilities.checkArgument(length > 0, "invalid maxQuadKeyLength=" + length);
		
		return maxQuadKeyLength(FOption.of(length));
	}
	public EstimateQuadKeysOptions maxQuadKeyLength(FOption<Integer> length) {
		return new EstimateQuadKeysOptions(m_mapperCount, m_validRange, m_sampleSize, length,
											m_clusterSize);
	}
	
	public FOption<Long> clusterSize() {
		return m_clusterSize;
	}
	public EstimateQuadKeysOptions clusterSize(FOption<Long> clusterSize) {
		return new EstimateQuadKeysOptions(m_mapperCount, m_validRange, m_sampleSize,
											m_maxQuadKeyLength, clusterSize);
	}
	public EstimateQuadKeysOptions clusterSize(long clusterSize) {
		Utilities.checkArgument(clusterSize > 0, "invalid maxClusterSize=" + clusterSize);
		
		return clusterSize(FOption.of(clusterSize));
	}
	
	@Override
	public String toString() {
		List<String> parts = Lists.newArrayList();

		m_mapperCount.ifPresent(c -> parts.add("mapper_count=" + c));
		m_maxQuadKeyLength.ifPresent(l -> parts.add("quadkey_length=" + l));
		m_clusterSize.map(UnitUtils::toByteSizeString).ifPresent(sz -> parts.add("cluster_size=" + sz));
		m_validRange.ifPresent(r -> parts.add("valid_range=" + r));
		
		return FStream.from(parts).join(", ");
	}
	
	public static EstimateQuadKeysOptions fromProto(EstimateQuadKeysOptionsProto proto) {
		EstimateQuadKeysOptions opts = EstimateQuadKeysOptions.DEFAULT();
		
		switch ( proto.getOptionalMapperCountCase() ) {
			case MAPPER_COUNT:
				opts = opts.mapperCount(proto.getMapperCount());
				break;
			case OPTIONALMAPPERCOUNT_NOT_SET:
				break;
			default:
				throw new AssertionError();
		}
		
		switch ( proto.getOptionalValidRangeCase() ) {
			case VALID_RANGE:
				opts = opts.validRange(PBUtils.fromProto(proto.getValidRange()));
				break;
			case OPTIONALVALIDRANGE_NOT_SET:
				break;
			default:
				throw new AssertionError();
		}
		
		switch ( proto.getOptionalSampleSizeCase() ) {
			case SAMPLE_SIZE:
				opts = opts.sampleSize(proto.getSampleSize());
				break;
			case OPTIONALSAMPLESIZE_NOT_SET:
				break;
			default:
				throw new AssertionError();
		}
		
		switch ( proto.getOptionalMaxQuadKeyLengthCase() ) {
			case MAX_QUAD_KEY_LENGTH:
				opts = opts.maxQuadKeyLength(proto.getMaxQuadKeyLength());
				break;
			case OPTIONALMAXQUADKEYLENGTH_NOT_SET:
				break;
			default:
				throw new AssertionError();
		}
		
		switch ( proto.getOptionalClusterSizeCase() ) {
			case CLUSTER_SIZE:
				opts = opts.clusterSize(proto.getClusterSize());
				break;
			case OPTIONALCLUSTERSIZE_NOT_SET:
				break;
			default:
				throw new AssertionError();
		}
		
		return opts;
	}

	@Override
	public EstimateQuadKeysOptionsProto toProto() {
		EstimateQuadKeysOptionsProto.Builder builder = EstimateQuadKeysOptionsProto.newBuilder();
		m_mapperCount.ifPresent(builder::setMapperCount);
		m_validRange.map(PBUtils::toProto).ifPresent(builder::setValidRange);
		m_sampleSize.ifPresent(builder::setSampleSize);
		m_maxQuadKeyLength.ifPresent(builder::setMaxQuadKeyLength);
		m_clusterSize.ifPresent(builder::setClusterSize);
		
		return builder.build();
	}
}
