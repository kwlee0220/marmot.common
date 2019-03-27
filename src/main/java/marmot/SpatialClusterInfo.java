package marmot;

import java.util.Objects;

import com.google.common.base.Preconditions;
import com.vividsolutions.jts.geom.Envelope;

import marmot.proto.service.SpatialClusterInfoProto;
import marmot.protobuf.PBUtils;
import marmot.support.PBSerializable;
import utils.UnitUtils;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SpatialClusterInfo implements PBSerializable<SpatialClusterInfoProto> {
	private final String m_quadKey;
	private final Envelope m_tileBounds;
	private final Envelope m_dataBounds;
	private final int m_recordCount;
	private final int m_ownedRecordCount;
	private final long m_length;
	
	public SpatialClusterInfo(String quadKey, Envelope tileBounds, Envelope dataBounds,
							int count, int ownedCount, long length) {
		Objects.requireNonNull(quadKey, "quadKey is null");
		Objects.requireNonNull(tileBounds, "tile bounds is null");
		Objects.requireNonNull(dataBounds, "data bounds is null");
		Preconditions.checkArgument(count >= 0, "invalid count: " + count);
		Preconditions.checkArgument(ownedCount >= 0, "invalid owned-count: " + ownedCount);
		Preconditions.checkArgument(length >= 0, "invalid length: " + length);
		
		m_quadKey = quadKey;
		m_tileBounds = tileBounds;
		m_dataBounds = dataBounds;
		m_recordCount = count;
		m_ownedRecordCount = ownedCount;
		m_length = length;
	}
	
	public String getQuadKey() {
		return m_quadKey;
	}
	
	public Envelope getTileBounds() {
		return m_tileBounds;
	}
	
	public Envelope getDataBounds() {
		return m_dataBounds;
	}
	
	public int getRecordCount() {
		return m_recordCount;
	}
	
	public int getOwnedRecordCount() {
		return m_ownedRecordCount;
	}
	
	public long getByteLength() {
		return m_length;
	}

	public static SpatialClusterInfo fromProto(SpatialClusterInfoProto proto) {
		Envelope tileBounds = PBUtils.fromProto(proto.getTileBounds());
		Envelope dataBounds = PBUtils.fromProto(proto.getDataBounds());
		
		return new SpatialClusterInfo(proto.getQuadKey(), tileBounds, dataBounds,
									proto.getRecordCount(), proto.getOwnedRecordCount(),
									proto.getByteLength());
	}

	@Override
	public SpatialClusterInfoProto toProto() {
		return SpatialClusterInfoProto.newBuilder()
									.setQuadKey(m_quadKey)
									.setTileBounds(PBUtils.toProto(m_tileBounds))
									.setDataBounds(PBUtils.toProto(m_dataBounds))
									.setRecordCount(m_recordCount)
									.setOwnedRecordCount(m_ownedRecordCount)
									.setByteLength(m_length)
									.build();
	}
	
	@Override
	public String toString() {
		return String.format("SpatialCluster[qkey=%s, count=%d(%d), size=%s]", m_quadKey,
							m_recordCount, m_ownedRecordCount,
							UnitUtils.toByteSizeString(m_length));
	}
}