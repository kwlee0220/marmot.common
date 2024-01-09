package marmot.support;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import marmot.Column;
import marmot.ColumnNotFoundException;
import marmot.Record;
import marmot.RecordSchema;
import utils.stream.FStream;
import utils.stream.IntFStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RecordProjector {
	private final RecordSchema m_inputSchema;
	private final RecordSchema m_outputSchema;
	private final int[] m_colIdxes;
	private final Object[] m_values;
	
	public static RecordProjector of(RecordSchema schema, List<String> key) {
		int[] colIdxes = FStream.from(key)
								.mapToInt(colName -> schema.findColumn(colName)
												.map(Column::ordinal)
												.getOrElse(-1))
								.toArray();
		List<String> badKeyCols = new ArrayList<>();
		for ( int i =0; i < colIdxes.length; ++i ) {
			if ( colIdxes[i] < 0 ) {
				badKeyCols.add(key.get(i));
			}
		}
		if ( badKeyCols.size() > 0 ) {
			throw new ColumnNotFoundException("invalid columns to project: " + badKeyCols);
		}
		
		return new RecordProjector(schema, colIdxes);
	}
	
	public static RecordProjector of(RecordSchema schema, String... colNames) {
		return of(schema, Arrays.asList(colNames));
	}

	private RecordProjector(RecordSchema inputSchema, int[] colIdxes) {
		m_inputSchema = inputSchema;
		m_colIdxes = colIdxes;
		m_outputSchema = IntFStream.of(colIdxes)
									.mapToObj(inputSchema::getColumnAt)
									.fold(RecordSchema.builder(), (b,c) -> b.addColumn(c))
									.build();
		m_values = new Object[m_colIdxes.length];
	}
	
	public RecordSchema getInputRecordSchema() {
		return m_inputSchema;
	}
	
	public RecordSchema getOutputRecordSchema() {
		return m_outputSchema;
	}
	
	public void apply(Record input, Record output) {
		for ( int i =0; i < m_colIdxes.length; ++i ) {
			m_values[i] = input.get(m_colIdxes[i]);
		}
		
		output.setAll(m_values);
	}
	
	@Override
	public String toString() {
		return m_outputSchema.streamColumns().map(Column::name).join(",", "[", "]");
	}
}
