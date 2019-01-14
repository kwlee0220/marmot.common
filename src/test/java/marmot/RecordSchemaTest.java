package marmot;

import java.util.Collection;
import java.util.Iterator;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import marmot.protobuf.ProtoBufActivator;
import marmot.type.DataType;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RecordSchemaTest {
	
	@Before
	public void setup() {
	}
	
	@Test
	public void test01() throws Exception {
		RecordSchema schema = RecordSchema.EMPTY;
		
		Assert.assertEquals(0, schema.getColumnCount());
	}
	
	@Test
	public void test02() throws Exception {
		RecordSchema schema = RecordSchema.builder()
											.addColumn("col1", DataType.STRING)
											.addColumn("COL2", DataType.INT)
											.addColumn("Col3", DataType.DOUBLE)
											.build();

		Assert.assertEquals(3, schema.getColumnCount());
		Assert.assertEquals(true, schema.existsColumn("col1"));
		Assert.assertEquals(true, schema.existsColumn("Col1"));
		Assert.assertEquals(true, schema.existsColumn("col2"));
		Assert.assertEquals(true, schema.existsColumn("Col3"));
		
		Assert.assertEquals("col1", schema.getColumn("COL1").name());
	}
	
	@Test
	public void test03() throws Exception {
		RecordSchema schema = RecordSchema.EMPTY;
		RecordSchema schema2 = (RecordSchema)ProtoBufActivator.activate(schema.toProto());

		Assert.assertEquals(0, schema2.getColumnCount());
	}
	
	@Test
	public void test04() throws Exception {
		RecordSchema schema = RecordSchema.builder()
											.addColumn("col1", DataType.STRING)
											.addColumn("COL2", DataType.INT)
											.addColumn("Col3", DataType.DOUBLE)
											.build();
		
		Collection<Column> cols = schema.getColumnAll();
		Assert.assertEquals(3, cols.size());
		
		Iterator<Column> iter = cols.iterator();
		Assert.assertEquals("col1", iter.next().name());
		Assert.assertEquals("COL2", iter.next().name());
		Assert.assertEquals("Col3", iter.next().name());
		Assert.assertEquals(false, iter.hasNext());
	}
	
	@Test
	public void test05() throws Exception {
		RecordSchema schema = RecordSchema.builder()
											.addColumn("col1", DataType.STRING)
											.addColumn("COL2", DataType.INT)
											.addColumn("Col3", DataType.DOUBLE)
											.build();
		
		RecordSchema schema2 = schema.toBuilder()
										.addOrReplaceColumn("col2", DataType.SHORT)
										.build();

		Assert.assertEquals(3, schema2.getColumnCount());
		Assert.assertEquals(true, schema2.existsColumn("Col1"));
		Assert.assertEquals(true, schema2.existsColumn("col2"));
		Assert.assertEquals(true, schema2.existsColumn("Col3"));
		Assert.assertEquals(DataType.SHORT, schema2.getColumn("cOL2").type());
		
		RecordSchema schema3 = schema.toBuilder()
										.addOrReplaceColumn("cOl4", DataType.SHORT)
										.build();

		Assert.assertEquals(4, schema3.getColumnCount());
		Assert.assertEquals(true, schema3.existsColumn("Col1"));
		Assert.assertEquals(true, schema3.existsColumn("col2"));
		Assert.assertEquals(true, schema3.existsColumn("Col3"));
		Assert.assertEquals(true, schema3.existsColumn("col4"));
		Assert.assertEquals(DataType.SHORT, schema3.getColumn("col4").type());
	}
}
