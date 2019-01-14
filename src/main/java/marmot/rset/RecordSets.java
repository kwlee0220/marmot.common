package marmot.rset;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.base.Preconditions;

import io.reactivex.Observable;
import io.reactivex.Observer;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import utils.func.FOption;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RecordSets {
	public static final RecordSet NULL = RecordSet.empty(RecordSchema.EMPTY);
	
	private RecordSets() {
		throw new AssertionError("Should not be called: class=" + RecordSets.class);
	}
	
	
	
//	public static RecordSet from(RecordSchema schema, Observable<Record> records) {
//		PipedRecordSet pipe = new PipedRecordSet(schema);
//		records
//			.subscribeOn(Schedulers.io())
//			.subscribe(pipe::supply, pipe::endOfSupply, pipe::endOfSupply);
//		
//		return pipe;
//	}
	
	public static RecordSet from(RecordSchema schema, Observable<Record> records,
								int queueLength) {
		PipedRecordSet pipe = new PipedRecordSet(schema, queueLength);
		records.subscribe(pipe::supply, pipe::endOfSupply, pipe::endOfSupply);
		
		return pipe;
	}
	
	public static RecordSet attachCloser(RecordSet rset, Runnable closer) {
		Objects.requireNonNull(rset, "Base RecordSet");
		Objects.requireNonNull(closer, "Closer");
		
		return new CloserAttachedRecordSet(rset, closer);
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
	
	public static RecordSet concat(RecordSchema schema, Collection<? extends RecordSet> rsets) {
		return concat(schema, FStream.of(rsets));
	}
	
	public static RecordSet concat(RecordSchema schema, FStream<? extends RecordSet> rsets) {
		return ConcatedRecordSet.concat(schema, rsets);
	}
	
	public static RecordSet concat(RecordSet rset1, RecordSet rset2) {
		Objects.requireNonNull(rset1);
		
		return concat(rset1.getRecordSchema(), Arrays.asList(rset1, rset2));
	}
	
	public static RecordSet concat(RecordSchema schema, RecordSet... rsets) {
		return concat(schema, Arrays.asList(rsets));
	}
	
//	public static PipedRecordSet pipe(RecordSchema schema) {
//		return new PipedRecordSet(schema);
//	}
	
	public static PipedRecordSet pipe(RecordSchema schema, int queueLength) {
		return new PipedRecordSet(schema, queueLength);
	}
	
	public static PushBackableRecordSet toPushBackable(RecordSet rset) {
		return (rset instanceof PushBackableRecordSet)
				? (PushBackableRecordSet)rset
				: new PushBackableRecordSetImpl(rset);
	}
	
	public static TransformedRecordSet map(RecordSet rset, Function<Record,Record> transform) {
		return new TransformedRecordSet(rset, transform);
	}
	
	public static FlatTransformedRecordSet flatMap(RecordSet rset, RecordSchema outSchema,
													Function<Record,RecordSet> transform) {
		return new FlatTransformedRecordSet(rset, outSchema, transform);
	}
	
	public static RecordSet autoClose(RecordSet input) {
		return new AutoClosingRecordSet(input);
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
}
