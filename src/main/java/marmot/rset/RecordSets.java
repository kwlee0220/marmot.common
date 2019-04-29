package marmot.rset;

import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import io.reactivex.Observable;
import io.reactivex.Observer;
import marmot.Column;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.rset.ConcatedRecordSet.FStreamConcatedRecordSet;
import utils.Utilities;
import utils.func.FOption;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RecordSets {
	public static final RecordSet NULL = RecordSet.empty(RecordSchema.NULL);
	
	private RecordSets() {
		throw new AssertionError("Should not be called: class=" + RecordSets.class);
	}
	
	public static RecordSet from(RecordSchema schema, Observable<Record> records,
								int queueLength) {
		PipedRecordSet pipe = new PipedRecordSet(schema, queueLength);
		records.subscribe(pipe::supply, pipe::endOfSupply, pipe::endOfSupply);
		
		return pipe;
	}
	
	public static RecordSet attachCloser(RecordSet rset, Runnable closer) {
		Objects.requireNonNull(rset, "Base RecordSet");
		Objects.requireNonNull(closer, "Closer");
		
		return new AbstractRecordSet() {
			@Override
			protected void closeInGuard() {
				rset.closeQuietly();
				
				synchronized ( this ) {
					closer.run();
				}
			}

			@Override
			public RecordSchema getRecordSchema() {
				return rset.getRecordSchema();
			}
			
			@Override
			public boolean next(Record record) {
				return rset.next(record);
			}
			
			@Override
			public Record nextCopy() {
				return rset.nextCopy();
			}
		};
	}
	
	public static RecordSet lazy(RecordSchema schema, Supplier<RecordSet> supplier) {
		return new LazyRecordSet(schema, supplier);
	}
	
	public static RecordSet singleton(RecordSchema schema, Consumer<Record> setter) {
		return new LazySingletonRecordSet(schema, setter);
	}
	private static class LazySingletonRecordSet extends AbstractRecordSet {
		private final RecordSchema m_schema;
		private Consumer<Record> m_recordSetter;	// null 이면 EOS로 간주
		
		private LazySingletonRecordSet(RecordSchema schema, Consumer<Record> setter) {
			m_schema = schema;
			m_recordSetter = setter;
		}
		
		@Override protected void closeInGuard() { }

		@Override
		public RecordSchema getRecordSchema() {
			return m_schema;
		}

		@Override
		public boolean next(Record record) {
			checkNotClosed();
			
			if ( m_recordSetter != null ) {
				m_recordSetter.accept(record);
				m_recordSetter = null;
				return true;
			}
			else {
				return false;
			}
		}
	}
	
	public static PeekableRecordSet toPeekable(RecordSet rset) {
		return new PeekableRecordSet(rset);
	}
	
	public static FOption<RecordSet> asNonEmpty(RecordSet rset) {
		PeekableRecordSet peekable = RecordSets.toPeekable(rset);
		
		if ( peekable.hasNext() ) {
			return FOption.of(peekable);
		}
		else {
			return FOption.empty();
		}
	}
	
	public static RecordSet concat(Record head, RecordSet tail) {
		Objects.requireNonNull(head);
		Objects.requireNonNull(tail);
//		Preconditions.checkArgument(head.getSchema().equals(tail.getRecordSchema()));
		
		return concat(tail.getRecordSchema(), RecordSet.of(head), tail);
	}
	
	public static RecordSet concat(RecordSet rset1, Record tail) {
		Objects.requireNonNull(rset1);
		Objects.requireNonNull(tail);
		Preconditions.checkArgument(rset1.getRecordSchema().equals(tail.getRecordSchema()));
		
		return concat(rset1.getRecordSchema(), rset1, RecordSet.of(tail));
	}
	
	public static RecordSet concat(RecordSchema schema, Iterable<? extends RecordSet> rsets) {
		Utilities.checkNotNullArgument(rsets, "rsets is null");
		
		return concat(schema, FStream.from(rsets));
	}
	
	public static RecordSet concat(RecordSchema schema, FStream<? extends RecordSet> rsets) {
		Utilities.checkNotNullArgument(schema, "schema is null");
		Utilities.checkNotNullArgument(rsets, "rsets is null");
		
		return new FStreamConcatedRecordSet(schema, rsets);
	}
	
	public static RecordSet concat(RecordSet rset1, RecordSet rset2) {
		Utilities.checkNotNullArgument(rset1, "rset1 is null");
		Utilities.checkNotNullArgument(rset2, "rset2 is null");
		
		return concat(rset1.getRecordSchema(), FStream.of(rset1, rset2));
	}
	
	public static RecordSet concat(RecordSchema schema, RecordSet... rsets) {
		Utilities.checkNotNullArguments(rsets, "rsets is null");
		
		return concat(schema, FStream.of(rsets));
	}
	
	public static PipedRecordSet pipe(RecordSchema schema, int queueLength) {
		return new PipedRecordSet(schema, queueLength);
	}
	
	public static PushBackableRecordSet toPushBackable(RecordSet rset) {
		return (rset instanceof PushBackableRecordSet)
				? (PushBackableRecordSet)rset
				: new PushBackableRecordSetImpl(rset);
	}
	
	public static RecordSet filter(RecordSet rset, Predicate<Record> pred) {
		Utilities.checkNotNullArgument(pred, "pred is null");
		
		return new AbstractRecordSet() {
			@Override
			public RecordSchema getRecordSchema() {
				return rset.getRecordSchema();
			}

			@Override
			protected void closeInGuard() throws Exception {
				rset.closeQuietly();
			}
			
			@Override
			public boolean next(Record output) {
				while ( next(output) ) {
					if ( pred.test(output) ) {
						return true;
					}
				}
				
				return false;
			}
			
			@Override
			public Record nextCopy() {
				Record rec;
				while ( (rec = nextCopy()) != null ) {
					if ( pred.test(rec) ) {
						return rec;
					}
				}
				
				return null;
			}
		};
	}
	
	public static FlatTransformedRecordSet flatMap(RecordSet rset, RecordSchema outSchema,
													Function<Record,RecordSet> transform) {
		return new FlatTransformedRecordSet(rset, outSchema, transform);
	}
	
	public static RecordSet autoClose(RecordSet input) {
		return new AbstractRecordSet() {
			@Override
			public void closeInGuard() {
				input.close();
			}

			@Override
			public RecordSchema getRecordSchema() {
				return input.getRecordSchema();
			}
			
			@Override
			public boolean next(Record record) {
				boolean done = input.next(record);
				if ( !done ) {
					close();
				}
				
				return done;
			}
			
			@Override
			public Record nextCopy() {
				Record next = input.nextCopy();
				if ( next == null ) {
					close();
					return null;
				}
				else {
					return next;
				}
			}
		};
	}
	
	public static ProgressReportingRecordSet reportProgress(RecordSet rset, Observer<Long> observer,
															long interval) {
		return new ProgressReportingRecordSet(rset, observer, interval);
	}
	
	public static Observable<Record> observe(RecordSet rset) {
		return Observable.create(new RecordSetSubscriber(rset));
	}
	
	public static CountingRecordSet toCountingRecordSet(RecordSet rset) {
		return new CountingRecordSet(rset);
	}
	public static class CountingRecordSet extends AbstractRecordSet {
		private final RecordSet m_rset;
		private long m_count = 0;
		
		CountingRecordSet(RecordSet rset) {
			Objects.requireNonNull(rset, "input RecordSet is null");
			
			m_rset = rset;
		}

		@Override
		protected void closeInGuard() {
			m_rset.closeQuietly();
		}

		@Override
		public RecordSchema getRecordSchema() {
			return m_rset.getRecordSchema();
		}
		
		public long getCount() {
			return m_count;
		}
		
		@Override
		public boolean next(Record output) {
			if ( m_rset.next(output) ) {
				++m_count;
				return true;
			}
			else {
				return false;
			}
		}
		
		@Override
		public Record nextCopy() {
			Record next = m_rset.nextCopy();
			if ( next != null ) {
				++m_count;
			}
			
			return next;
		}
		
		@Override
		public String toString() {
			return m_rset.toString() + "(" + m_count + ")";
		}
	}
	
	public static class RenamedRecordSet extends AbstractRecordSet {
		private final RecordSet m_src;
		private final RecordSchema m_schema;
		
		public RenamedRecordSet(RecordSet src, Map<String,String> map) {
			m_src = src;
			
			Map<String,String> remains = Maps.newHashMap(map);
			m_schema = src.getRecordSchema().streamColumns()
							.map(c -> {
								String newName = remains.remove(c.name());
								if ( newName != null ) {
									return new Column(newName, c.type());
								}
								else {
									return c;
								}
							})
							.collectLeft(RecordSchema.builder(), (b,c) -> b.addColumn(c))
							.build();
		}

		@Override
		protected void closeInGuard() throws Exception {
			m_src.close();
		}

		@Override
		public RecordSchema getRecordSchema() {
			return m_schema;
		}

		@Override
		public boolean next(Record output) {
			return m_src.next(output);
		}

		@Override
		public Record nextCopy() {
			return m_src.nextCopy();
		}
		
	}
}
