package marmot.support;

import java.util.Objects;

import marmot.RecordSchema;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RecordSchemas {
	private RecordSchemas() {
		throw new AssertionError("Should not be called: " + RecordSchemas.class);
	}

	public static RecordSchema concat(RecordSchema... schemas) {
		Objects.requireNonNull(schemas, "RecordSchemas are null");
		
		return FStream.of(schemas)
					.flatMap(s -> FStream.of(s.getColumnAll()))
					.foldLeft(RecordSchema.builder(), (b,c) -> b.addColumn(c))
					.build();
	}
}
