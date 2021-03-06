package marmot.support;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import marmot.ColumnNotFoundException;
import marmot.Record;
import marmot.RecordSchema;
import marmot.type.DataType;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DefaultRecordTest {
	private RecordSchema m_schema;
	private Record m_record;
	
	@Before
	public void setup() {
		m_schema = RecordSchema.builder()
								.addColumn("col1", DataType.STRING)
								.addColumn("COL2", DataType.INT)
								.addColumn("Col3", DataType.DOUBLE)
								.build();
		m_record = DefaultRecord.of(m_schema);
		m_record.setAll("aaa", 15, 12.5d);
	}
	
	@Test
	public void test01() throws Exception {
		Assert.assertEquals(3, m_record.getColumnCount());
		Assert.assertEquals(m_schema, m_record.getRecordSchema());
		
		Assert.assertEquals("aaa", m_record.get(0));
		Assert.assertEquals(15, m_record.get(1));
		Assert.assertEquals(12.5d, m_record.get(2));
	}
	
	@Test
	public void test02() throws Exception {
		Assert.assertEquals("aaa", m_record.get("coL1"));
		Assert.assertEquals(15, m_record.get("cOl2"));
		Assert.assertEquals(12.5d, m_record.get("COL3"));
	}
	
	@Test
	public void test03() throws Exception {
		Object[] v = new Object[]{"aaa", 15, 12.5d};
		Assert.assertArrayEquals(v, m_record.getAll());
		
		Object[] v2 = new Object[]{"bbb", 30, 31.7d};
		m_record.setAll(v2);
		Assert.assertArrayEquals(v2, m_record.getAll());
	}
	
	@Test(expected=ColumnNotFoundException.class)
	public void test04() throws Exception {
		m_record.get(3);
	}
	
	@Test(expected=ColumnNotFoundException.class)
	public void test05() throws Exception {
		m_record.get("col4");
	}
	
	@Test
	public void test06() throws Exception {
		Object[] v = new Object[]{30, 31.7d};
		m_record.setAll(1, v);
		
		Object[] v2 = new Object[]{"aaa", 30, 31.7d};
		Assert.assertArrayEquals(v2, m_record.getAll());
	}
	
	@Test
	public void test07() throws Exception {
		Record rec2 = DefaultRecord.of(m_schema);
		rec2.setAll("a", 1, 2d);
		
		m_record.set(rec2);
		Assert.assertEquals("a", m_record.get("coL1"));
		Assert.assertEquals(1, m_record.get("cOl2"));
		Assert.assertEquals(2d, m_record.get("COL3"));
		
		RecordSchema schema2 = RecordSchema.builder()
											.addColumn("cOl1", DataType.STRING)
											.addColumn("Col3", DataType.DOUBLE)
											.addColumn("CoL4", DataType.SHORT)
											.build();
		rec2 = DefaultRecord.of(schema2);
		rec2.setAll("a", 3.14d, 17);

		m_record.setAll("aaa", 15, 12.5d);
		m_record.set(rec2);
		Assert.assertEquals("a", m_record.get("cOL1"));
		Assert.assertEquals(15, m_record.get("cOl2"));
		Assert.assertEquals(3.14d, m_record.get("COL3"));
	}
}
