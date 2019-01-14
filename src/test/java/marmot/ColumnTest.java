package marmot;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import marmot.type.DataType;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ColumnTest {
	
	@Before
	public void setup() {
	}
	
	@Test
	public void test01() throws Exception {
		Column col1 = new Column("col1", DataType.STRING);
		Column col2 = new Column("col2", DataType.INT);
		
		Assert.assertEquals("col1", col1.name());
		Assert.assertEquals("col2", col2.name());
		
		Assert.assertEquals(DataType.STRING, col1.type());
		Assert.assertEquals(DataType.INT, col2.type());
	}
	
	@Test(expected=NullPointerException.class)
	public void test02() throws Exception {
		new Column(null, DataType.STRING);
	}
	
	@Test(expected=NullPointerException.class)
	public void test03() throws Exception {
		new Column("col1", null);
	}
	
	@Test
	public void test05() throws Exception {
		Column col1 = new Column("col1", DataType.STRING);
		Column col2 = new Column("COL1", DataType.STRING);
		
		Assert.assertEquals(col1, col2);
		Assert.assertEquals(col1.hashCode(), col2.hashCode());
	}
	
	@Test
	public void test06() throws Exception {
		Column col1 = new Column("Col1", DataType.STRING);
		Column col2 = new Column("coL2", DataType.INT);
		
		Assert.assertEquals("Col1:string", col1.toString());
		Assert.assertEquals("coL2:int", col2.toString());
		
		Column col3 = Column.parse("COL1:string");
		Assert.assertEquals(col1, col3);
	}
}
