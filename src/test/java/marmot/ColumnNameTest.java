package marmot;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ColumnNameTest {
	
	@Before
	public void setup() {
	}
	
	@Test
	public void test01() throws Exception {
		ColumnName name1 = new ColumnName("col1");
		ColumnName name2 = new ColumnName("COL1");
		ColumnName name3 = new ColumnName("COL2");
		
		Assert.assertEquals(name1, name2);
		Assert.assertEquals(name1.hashCode(), name2.hashCode());
		Assert.assertNotEquals(name1.get(), name2.get());
		Assert.assertEquals(0, name1.compareTo(name2));
		
		Assert.assertNotEquals(name1, name3);
	}
}
