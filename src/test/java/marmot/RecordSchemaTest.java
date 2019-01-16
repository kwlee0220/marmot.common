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
		RecordSchema schema = RecordSchema.NULL;
		
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
		Assert.assertEquals(true, schema.existsColumn(ColumnName.of("col1")));
		Assert.assertEquals(true, schema.existsColumn(ColumnName.of("Col1")));
		Assert.assertEquals(true, schema.existsColumn(ColumnName.of("col2")));
		Assert.assertEquals(true, schema.existsColumn(ColumnName.of("Col3")));
		
		Assert.assertEquals("col1", schema.getColumn("COL1").name().get());
	}
	
	@Test
	public void test03() throws Exception {
		RecordSchema schema = RecordSchema.NULL;
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
		Assert.assertEquals("col1", iter.next().name().get());
		Assert.assertEquals("COL2", iter.next().name().get());
		Assert.assertEquals("Col3", iter.next().name().get());
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
		Assert.assertEquals(true, schema2.existsColumn(ColumnName.of("Col1")));
		Assert.assertEquals(true, schema2.existsColumn(ColumnName.of("col2")));
		Assert.assertEquals(true, schema2.existsColumn(ColumnName.of("Col3")));
		Assert.assertEquals(DataType.SHORT, schema2.getColumn("cOL2").type());
		
		RecordSchema schema3 = schema.toBuilder()
										.addOrReplaceColumn("cOl4", DataType.SHORT)
										.build();

		Assert.assertEquals(4, schema3.getColumnCount());
		Assert.assertEquals(true, schema3.existsColumn(ColumnName.of("Col1")));
		Assert.assertEquals(true, schema3.existsColumn(ColumnName.of("col2")));
		Assert.assertEquals(true, schema3.existsColumn(ColumnName.of("Col3")));
		Assert.assertEquals(true, schema3.existsColumn(ColumnName.of("col4")));
		Assert.assertEquals(DataType.SHORT, schema3.getColumn("col4").type());
	}
	
	@Test
	public void test06() throws Exception {
		String str = "col1:string,COL2:int,Col3:double";
		RecordSchema schema = RecordSchema.parse(str);
		
		Collection<Column> cols = schema.getColumnAll();
		Assert.assertEquals(3, cols.size());
		
		Iterator<Column> iter = cols.iterator();
		Assert.assertEquals("col1", iter.next().name().get());
		Assert.assertEquals("COL2", iter.next().name().get());
		Assert.assertEquals("Col3", iter.next().name().get());
		Assert.assertEquals(false, iter.hasNext());
	}
}
