package marmot.support;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface TypedObject {
	public void readFields(DataInput input) throws IOException;
	public void writeFields(DataOutput output) throws IOException;
	
	public default void readFrom(InputStream is) throws IOException {
		readFields(new DataInputStream(is));
	}
	
	public default void writeInto(OutputStream os) throws IOException {
		writeFields(new DataOutputStream(os));
	}
	
	public default void readFrom(byte[] bytes) throws IOException {
		try ( ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
				DataInputStream input = new DataInputStream(bais) ) {
			readFields(input);
		}
	}
	
	public default byte[] toBytes() throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try ( DataOutputStream output = new DataOutputStream(baos) ) {
			writeFields(output);
		}
		return baos.toByteArray();
	}
}
