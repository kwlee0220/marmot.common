package marmot.externio;

import java.io.Closeable;
import java.io.IOException;

import marmot.DataSet;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.support.DefaultRecord;
import utils.Utilities;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface RecordWriter extends RecordSetWriter, Closeable {
	public RecordSchema getRecordSchema();
	
	/**
	 * 레코드 세트에 포함된 모든 레코드를 저장한다.
	 * 
	 * @param rset	저장할 레코드를 포함한 레코드 세트 객체.
	 * @return	저장된 레코드의 갯수.
	 * @throws IOException	저장 도중 예외가 발생된 경우.
	 */
	public void write(Record rset) throws IOException;

	/**
	 * 레코드 세트에 포함된 모든 레코드를 저장한다.
	 * 
	 * @param ds	저장할 레코드를 포함한 레코드 세트 객체.
	 * @return	저장된 레코드의 갯수.
	 * @throws IOException	저장 도중 예외가 발생된 경우.
	 */
	public default long write(RecordSet rset) throws IOException {
		Utilities.checkNotNullArgument(rset, "RecordSet is null");
		
		long nwrites = 0;
		Record record = DefaultRecord.of(getRecordSchema());
		
		try {
			while ( rset.next(record) ) {
				write(record);
				++nwrites;
			}
			
			return nwrites;
		}
		finally {
			rset.close();
		}
	}

	/**
	 * 데이터 세트에 포함된 모든 레코드를 저장한다.
	 * 
	 * @param ds	저장할 레코드를 포함한 데이터 세트 객체.
	 * @return	저장된 레코드의 갯수.
	 * @throws IOException	저장 도중 예외가 발생된 경우.
	 */
	public default long write(DataSet ds) throws IOException {
		try ( RecordSet rset = ds.read() ) {
			return write(rset);
		}
	}
}
