package marmot.rset;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.base.Preconditions;

import io.reactivex.Observable;
import io.reactivex.Observer;
import io.vavr.control.Option;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import utils.stream.FStream;
import utils.stream.PeekableFStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RecordSets {
	public static final RecordSet NULL = empty(RecordSchema.EMPTY);
	
	private RecordSets() {
		throw new AssertionError();
	}
	
	/**
	 * 빈 레코드세트 객체를 생성한다.
	 * 
	 * @param schema	레코드 세트의 스키마.
	 * @return		{@link RecordSet} 객체.
	 */
	public static RecordSet empty(RecordSchema schema) {
		return new EmptyRecordSet(schema);
	}
	
	/**
	 * 주어진 레코드들로 구성된 레코드 세트를 생성한다.
	 * <p>
	 * 올바른 동작을 위해서는 인자인 {@code schema}와 레코드들의 스키마는 동일하여야 한다.
	 * 
	 * @param schema	생성될 레코드 세트의 스키마.
	 * @param records	레코드 세트에 포함될 레코드 집합.
	 * @return	레코드 세트
	 */
	public static RecordSet from(RecordSchema schema, Iterable<? extends Record> records) {
		Objects.requireNonNull(records);
		
		return new IteratorRecordSet(schema, records.iterator());
	}
	
	/**
	 * 주어진 레코드들로 구성된 레코드 세트를 생성한다.
	 * <p>
	 * 레코드 집합에는 반드시 하나 이상의 레코드가 포함되어야 한다.
	 * 
	 * @param records	레코드 세트에 포함될 레코드 집합.
	 * @return	레코드 세트
	 * @throws IllegalArgumentException	입력 레코드 집합이 빈 경우.
	 */
	public static RecordSet from(Iterable<? extends Record> records) {
		Objects.requireNonNull(records);
		
		Iterator<? extends Record> iter = records.iterator();
		Preconditions.checkArgument(iter.hasNext(), "records is empty");
		
		RecordSchema schema = iter.next().getSchema();
		return from(schema, records.iterator());
	}
	
	/**
	 * 주어진 레코드의 Iterator로부터 레코드 세트를 생성한다.
	 * <p>
	 * 올바른 동작을 위해서는 인자인 {@code schema}와 레코드들의 스키마는 동일하여야 한다.
	 * 
	 * @param schema	생성될 레코드 세트의 스키마.
	 * @param records	레코드 세트에 포함될 레코드 집합.
	 * @return	레코드 세트
	 */
	public static RecordSet from(RecordSchema schema, Iterator<? extends Record> records) {
		return new IteratorRecordSet(schema, records);
	}
	
	public static RecordSet from(RecordSchema schema, FStream<Record> fstream) {
		Objects.requireNonNull(schema, "RecordSchema is null");
		Objects.requireNonNull(fstream, "FStream is null");
		
		return new FStreamRecordSet(schema, fstream);
	}
	
	public static RecordSet from(FStream<Record> fstream) {
		Objects.requireNonNull(fstream, "FStream is null");
		
		return new FStreamRecordSet(fstream);
	}
	
	/**
	 * 단일 레코드로 구성된 레코드 세트를 생성한다.
	 * 
	 * @param record	레코드 세트에 포함될 레코드.
	 * @return	레코드 세트
	 */
	public static RecordSet of(Record record) {
		Objects.requireNonNull(record, "record is null");
		
		List<Record> records = Collections.singletonList(record);
		return RecordSets.from(record.getSchema(), records);
	}
	
	public static RecordSet from(Record... records) {
		Objects.requireNonNull(records, "records is null");
		Preconditions.checkArgument(records.length > 0, "records are empty");
		
		RecordSchema schema = records[0].getSchema();
		return from(schema, Arrays.asList(records));
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
	
	public static RecordSet asNonEmpty(RecordSet rset) {
		PeekableRecordSet peekable = RecordSets.toPeekable(rset);
		if ( peekable.peek().isDefined() ) {
			return peekable;
		}
		else {
			return null;
		}
	}
	
	public static RecordSet concat(Record head, RecordSet tail) {
		Objects.requireNonNull(head);
		Objects.requireNonNull(tail);
//		Preconditions.checkArgument(head.getSchema().equals(tail.getRecordSchema()));
		
		return concat(tail.getRecordSchema(), RecordSets.from(head), tail);
	}
	
	public static RecordSet concat(RecordSet rset1, Record tail) {
		Objects.requireNonNull(rset1);
		Objects.requireNonNull(tail);
		Preconditions.checkArgument(rset1.getRecordSchema().equals(tail.getSchema()));
		
		return concat(rset1.getRecordSchema(), rset1, RecordSets.from(tail));
	}
	
	public static RecordSet concat(RecordSchema schema, Collection<? extends RecordSet> rsets) {
		return concat(schema, FStream.of(rsets));
	}
	
	public static RecordSet concat(Collection<? extends RecordSet> rsets) {
		
		
		return concat(FStream.of(rsets));
	}
	
	public static RecordSet concat(RecordSchema schema, FStream<? extends RecordSet> rsets) {
		return ConcatedRecordSet.concat(schema, rsets);
	}
	
	public static RecordSet concat(FStream<? extends RecordSet> rsets) {
		PeekableFStream<? extends RecordSet> peekable = rsets.toPeekable();
		RecordSet first = peekable.next();
		if ( first == null ) {
			throw new IllegalArgumentException("no components RecordSet");
		}
		
		return concat(first.getRecordSchema(), peekable);
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
		return new AutoCloseRecordSet(input);
	}
	
	public static CloserAttachedRecordSet attachCloser(RecordSet rset, Runnable closer) {
		return new CloserAttachedRecordSet(rset, closer);
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
		public Option<Record> nextCopy() {
			return m_rset.nextCopy()
						.peek(r -> ++m_count);
		}
		
		@Override
		public String toString() {
			return m_rset.toString() + "(" + m_count + ")";
		}
	}
}
