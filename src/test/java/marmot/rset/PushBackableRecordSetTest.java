package marmot.rset;


import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.support.DefaultRecord;
import marmot.type.DataType;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PushBackableRecordSetTest {
	private static RecordSchema s_schema;
	private Record m_record;
	
	@BeforeClass
	public static void setupForClass() {
		s_schema = RecordSchema.builder()
								.addColumn("col1", DataType.INT)
								.build();
	}
	
	@Before
	public void setup() {
		m_record = DefaultRecord.of(s_schema);
	}
	
	@Test
	public void test0() throws Exception {
		RecordSet rset0 = RecordSet.of(m_record.set(0, 0).duplicate(),
										m_record.set(0, 1).duplicate(),
										m_record.set(0, 2).duplicate());
		PushBackableRecordSet rset = rset0.asPushBackable();
		
		int[] values = rset.fstream().mapToInt(r -> r.getInt(0)).toArray();
		Assert.assertArrayEquals(new int[] {0, 1, 2}, values);
	}
	
	@Test
	public void test1() throws Exception {
		RecordSet rset0 = RecordSet.of(m_record.set(0, 0).duplicate(),
										m_record.set(0, 1).duplicate(),
										m_record.set(0, 2).duplicate());
		PushBackableRecordSet rset = rset0.asPushBackable();
		Record r0 = rset.nextCopy();
		Assert.assertEquals(r0.getInt(0), 0);
		Assert.assertEquals(rset.peekCopy().getInt(0), 1);
		
		rset.pushBack(r0);
		Assert.assertEquals(rset.peekCopy().getInt(0), 0);

		Record r_1 = DefaultRecord.of(s_schema);
		r_1.set(0, -1);
		rset.pushBack(r_1);
		
		int[] values = rset.fstream().mapToInt(r -> r.getInt(0)).toArray();
		Assert.assertArrayEquals(new int[] {-1, 0, 1, 2}, values);
	}
	
	@Test
	public void test2() throws Exception {
		RecordSet rset0 = RecordSet.of(m_record.set(0, 0).duplicate(),
										m_record.set(0, 1).duplicate(),
										m_record.set(0, 2).duplicate());
		PushBackableRecordSet rset = rset0.asPushBackable();
		for ( int i =0; i < 3; ++i ) {
			rset.nextCopy();
		}
		Assert.assertEquals(null, rset.peekCopy());
	}
}
