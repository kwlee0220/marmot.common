package marmot.optor.geo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;

import marmot.geo.GeoClientUtils;
import marmot.support.TypedObject;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class BuildingPostalAddressInfo implements TypedObject {
	private Geometry m_geom;
	private String m_id;
	private String m_name;
	private String m_roadAddrCode;
	private String m_roadAddress;
	private String m_jibunAddrCode;
	private String m_jibunAddress;
	
	public String getId() {
		return m_id;
	}
	
	public void setId(String id) {
		m_id = id;
	}
	
	public Geometry getGeometry() {
		return m_geom;
	}
	
	public void setGeometry(Geometry geom) {
		m_geom = geom;
	}
	
	public String getName() {
		return m_name;
	}
	
	public void setName(String name) {
		m_name = name;
	}
	
	public String getRoadAddressCode() {
		return m_roadAddrCode.substring(0, 12);
	}
	
	public void setFullRoadAddressCode(String code) {
		m_roadAddrCode = code;
	}
	
	public short getBuildingNumber() {
		return Short.parseShort(m_roadAddrCode.substring(13, 17));
	}
	
	public short getBuildingSubNumber() {
		return Short.parseShort(m_roadAddrCode.substring(17, 21));
	}
	
	public byte getBuildingLevel() {
		return Byte.parseByte(m_roadAddrCode.substring(12, 12));
	}
	
	public String getRoadAddress() {
		return m_roadAddress;
	}
	
	public void setRoadAddress(String addr) {
		m_roadAddress = addr;
	}
	
	public String getJibunAddressCode() {
		return m_jibunAddrCode;
	}
	
	public void setJibunAddressCode(String code) {
		m_jibunAddrCode = code;
	}
	
	public String getJibunAddress() {
		return m_jibunAddress;
	}
	
	public void setJibunAddress(String addr) {
		m_jibunAddress = addr;
	}
	
	public String getSidCode() {
		return m_jibunAddrCode.substring(0, 2);
	}
	
	public String getSggCode() {
		return m_jibunAddrCode.substring(0, 5);
	}
	
	public String getEmdCode() {
		return m_jibunAddrCode.substring(0, 8);
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		try {
			byte[] wkb = new byte[input.readInt()];
			input.readFully(wkb);
			m_geom = GeoClientUtils.fromWKB(wkb);
			m_id = input.readUTF();
			String str = input.readUTF();
			m_name = (str.length() > 0) ? str : null;
			m_roadAddrCode = input.readUTF();
			m_roadAddress = input.readUTF();
			m_jibunAddrCode = input.readUTF();
			m_jibunAddress = input.readUTF();
		}
		catch ( ParseException e ) {
			throw new IOException("" + e);
		}
	}

	@Override
	public void writeFields(DataOutput output) throws IOException {
		byte[] wkb = GeoClientUtils.toWKB(m_geom);
		output.writeInt(wkb.length);
		output.write(wkb);
		output.writeUTF(m_id);
		if ( m_name != null ) {
			output.writeUTF(m_name);
		}
		else {
			output.writeUTF("");
		}
		output.writeUTF(m_roadAddrCode);
		output.writeUTF(m_roadAddress);
		output.writeUTF(m_jibunAddrCode);
		output.writeUTF(m_jibunAddress);
	}

	@Override
	public String toString() {
		return m_roadAddress;
	}
	
	@Override
	public boolean equals(Object o) {
		if ( o == this ) {
			return true;
		}
		else if ( o == null || !(o instanceof BuildingPostalAddressInfo) ) {
			return false;
		}
		
		BuildingPostalAddressInfo other = (BuildingPostalAddressInfo)o;
		return m_id.equals(other.m_id);
	}
	
	@Override
	public int hashCode() {
		return m_id.hashCode();
	}
}
