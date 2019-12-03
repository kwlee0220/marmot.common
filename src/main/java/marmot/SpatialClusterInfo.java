package marmot;

import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;

import com.google.common.base.Preconditions;
import com.vividsolutions.jts.geom.Envelope;

import marmot.proto.service.SpatialClusterInfoProto;
import marmot.protobuf.PBUtils;
import marmot.support.PBSerializable;
import utils.UnitUtils;
import utils.Utilities;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SpatialClusterInfo implements PBSerializable<SpatialClusterInfoProto>, Serializable {
	private static final long serialVersionUID = -3304936292546908854L;
	
	private final String m_quadKey;
	private final Envelope m_tileBounds;
	private final Envelope m_dataBounds;
	private final Envelope m_overlap;
	private final int m_recordCount;
	private final int m_ownedRecordCount;
	private final long m_length;
	
	public SpatialClusterInfo(String quadKey, Envelope tileBounds, Envelope dataBounds,
							int count, int ownedCount, long length) {
		Utilities.checkNotNullArgument(quadKey, "quadKey is null");
		Utilities.checkNotNullArgument(tileBounds, "tile bounds is null");
		Utilities.checkNotNullArgument(dataBounds, "data bounds is null");
		Preconditions.checkArgument(count >= 0, "invalid count: " + count);
		Preconditions.checkArgument(ownedCount >= 0, "invalid owned-count: " + ownedCount);
		Preconditions.checkArgument(length >= 0, "invalid length: " + length);
		
		m_quadKey = quadKey;
		m_tileBounds = tileBounds;
		m_dataBounds = dataBounds;
		m_overlap = tileBounds.intersection(dataBounds);
		m_recordCount = count;
		m_ownedRecordCount = ownedCount;
		m_length = length;
	}
	
	/**
	 * 공간 파티션에게 부여된 식별자를 반환한다.
	 * 
	 * @return	공간 파티션 식별자.
	 */
	public String getQuadKey() {
		return m_quadKey;
	}
	
	/**
	 * 본 공간 파티션 영역을 반환한다.
	 * 
	 * @return	공간 파티션 영역.
	 */
	public Envelope getTileBounds() {
		return m_tileBounds;
	}
	
	/**
	 * 본 공간 파티션에 저장된 모든 공간 데이터의 MBR 영역을 반환한다.
	 * 
	 * @return	공간 데이터의 MBR 영역
	 */
	public Envelope getDataBounds() {
		return m_dataBounds;
	}
	
	/**
	 * 본 공간 파티션에 저장된 모든 공간 데이터의 갯수를 반환한다.
	 * 
	 * @return	공간 데이터의 갯수
	 */
	public int getRecordCount() {
		return m_recordCount;
	}
	
	/**
	 * 본 공간 파티션에 저장된 공간 데이터들 중에서 본 파티션에 소속된
	 * 공간 데이터들의 수를 반환한다.
	 * 
	 * @return	공간 데이터의 갯수
	 */
	public int getOwnedRecordCount() {
		return m_ownedRecordCount;
	}
	
	public long getByteLength() {
		return m_length;
	}
	
	public boolean intersects(Envelope range) {
		return m_overlap.intersects(range);
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

	private Object writeReplace() {
		return new SerializationProxy(this);
	}
	
	private void readObject(ObjectInputStream stream) throws InvalidObjectException {
		throw new InvalidObjectException("Use Serialization Proxy instead.");
	}

	private static class SerializationProxy implements Serializable {
		private static final long serialVersionUID = 6194816506550906715L;
		
		private final SpatialClusterInfoProto m_proto;
		
		private SerializationProxy(SpatialClusterInfo info) {
			m_proto = info.toProto();
		}
		
		private Object readResolve() {
			return SpatialClusterInfo.fromProto(m_proto);
		}
	}
}