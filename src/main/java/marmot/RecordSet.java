package marmot;

import java.io.Closeable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import javax.annotation.Nullable;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.reactivex.Observable;
import io.reactivex.ObservableEmitter;
import io.reactivex.ObservableOnSubscribe;
import io.reactivex.Observer;
import marmot.RecordSets.AutoClosingRecordSet;
import marmot.RecordSets.CloserAttachedRecordSet;
import marmot.RecordSets.CountingRecordSet;
import marmot.RecordSets.EmptyRecordSet;
import marmot.RecordSets.FStreamRecordSet;
import marmot.RecordSets.FilteredRecordSet;
import marmot.RecordSets.IteratorRecordSet;
import marmot.RecordSets.LazyRecordSet;
import marmot.rset.FStreamConcatedRecordSet;
import marmot.rset.FlatTransformedRecordSet;
import marmot.rset.PipedRecordSet;
import marmot.rset.ProgressReportingRecordSet;
import marmot.rset.PushBackableRecordSet;
import marmot.rset.PushBackableRecordSetImpl;
import marmot.support.DefaultRecord;
import utils.LoggerSettable;
import utils.Throwables;
import utils.Utilities;
import utils.func.FOption;
import utils.func.Try;
import utils.func.Try.Failure;
import utils.func.Try.Success;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface RecordSet extends Closeable, Iterable<Record> {
	/**
	 * 본 레코드 세트에 속한 레코드들의 스키마를 반환한다.
	 * 
	 * @return	레코드 스키마.
	 */
	public RecordSchema getRecordSchema();
	
	/**
	 * 본 레코드 세트를 위해 할당된 자원을 반환한다.
	 */
	public void close();

	/**
	 * 본 레코드 세트를 위해 할당된 자원을 반환한다.
	 * <p>
	 * {@link #close} 연산 수행한 결과를 {@link Try}를 통해 반환된다.
	 * 성공적으로 close된 경우는 {@link Success}가 반환되고, 오류가 발생된
	 * 경우는 {@link Failure}를 반환한다.
	 * 
	 * @return	close 연산 성공 여부
	 */
	public default Try<Void> closeQuietly() {
		return Try.run(this::close);
	}
	
	/**
	 * 레코드 세트의 다음번 레코드를 읽어 주어진 레코드에 적재시킨다.
	 * 
	 * @param output	다음 레코드가 저장될 객체.
	 * @return	적재 여부. 레코드 세트에 더 이상의 레코드가 없는 경우는 false
	 */
	public boolean next(Record output);
	
	/**
	 * 레코드 세트의 다음번 레코드를 읽어 반환한다.
	 * 
	 * @return	읽은 레코드 객체.  레코드가 없는 경우는 {@code null}이 반환됨.
	 */
	public Record nextCopy();

	/**
	 * 빈 레코드세트 객체를 생성한다.
	 * 
	 * @param schema	레코드 세트의 스키마.
	 * @return		{@link RecordSet} 객체.
	 */
	public static RecordSet empty(RecordSchema schema) {
		Utilities.checkNotNullArgument(schema, "RecordSchema");
		
		return new EmptyRecordSet(schema);
	}

	/**
	 * 단일 레코드로 구성된 레코드 세트를 생성한다.
	 * 
	 * @param records	레코드 세트를 구성할 레코드 리스트.
	 * @return	레코드 세트
	 */
	public static RecordSet of(Record... records) {
		Utilities.checkNotNullArgument(records, "records is null");
		Utilities.checkArgument(records.length > 0, "records.length > 0, but: " + records.length);
		
		RecordSchema schema = records[0].getRecordSchema();
		return from(schema, Arrays.asList(records));
	}

	/**
	 * 레코드 스트림에 속한 레코드로부터 레코드 세트를 생성한다.
	 * 
	 * @param schema	레코드 스크마.
	 * @param fstream	레코드 스트림
	 * @return	레코드 세트
	 */
	public static RecordSet from(RecordSchema schema, FStream<? extends Record> fstream) {
		Utilities.checkNotNullArgument(schema, "RecordSchema is null");
		Utilities.checkNotNullArgument(fstream, "FStream is null");
		
		return new FStreamRecordSet(schema, fstream);
	}

	/**
	 * 주어진 레코드들로 구성된 레코드 세트를 생성한다.
	 * <p>
	 * 레코드 집합에는 반드시 하나 이상의 레코드가 포함되어야 한다.
	 * 
	 * @param records	레코드 세트에 포함될 레코드 집합.
	 * @return	레코드 세트
	 */
	public static RecordSet from(Iterable<? extends Record> records) {
		Utilities.checkNotNullArgument(records, "records is null");
		
		Iterator<? extends Record> iter = records.iterator();
		Utilities.checkArgument(iter.hasNext(), "Record Iterable is empty");
		
		RecordSchema schema = iter.next().getRecordSchema();
		return from(schema, records.iterator());
	}

	/**
	 * 주어진 레코드들로 구성된 레코드 세트를 생성한다.
	 * <p>
	 * 올바른 동작을 위해서는 인자인 {@code schema}와 레코드들의 스키마는 동일하여야 한다.
	 * 
	 * @param schema	생성될 레코드 세트의 스키마.
	 * @param iter	레코드 세트에 포함될 레코드 집합.
	 * @return	레코드 세트
	 */
	public static RecordSet from(RecordSchema schema, Iterable<? extends Record> iter) {
		Utilities.checkNotNullArgument(schema, "schema is null");
		Utilities.checkNotNullArgument(iter, "records is null");
		
		return from(schema, iter.iterator());
	}

	/**
	 * 주어진 레코드의 Iterator로부터 레코드 세트를 생성한다.
	 * <p>
	 * 올바른 동작을 위해서는 인자인 {@code schema}와 레코드들의 스키마는 동일하여야 한다.
	 * 
	 * @param schema	생성될 레코드 세트의 스키마.
	 * @param iter	레코드 세트에 포함될 레코드 집합.
	 * @return	레코드 세트
	 */
	public static RecordSet from(RecordSchema schema, Iterator<? extends Record> iter) {
		Utilities.checkNotNullArgument(schema, "schema is null");
		Utilities.checkNotNullArgument(iter, "records is null");
		
		return new IteratorRecordSet(schema, iter);
	}
	
	public static RecordSet from(RecordSchema schema, Observable<? extends Record> records,
								int queueLength) {
		PipedRecordSet pipe = new PipedRecordSet(schema, queueLength);
		records.subscribe(pipe::supply, pipe::endOfSupply, pipe::endOfSupply);
		
		return pipe;
	}
	
	public static PipedRecordSet pipe(RecordSchema schema, int queueLength) {
		return new PipedRecordSet(schema, queueLength);
	}
	
	public static RecordSet lazy(RecordSchema schema, Supplier<RecordSet> supplier) {
		return new LazyRecordSet(schema, supplier);
	}
	
	public default RecordSet filter(Predicate<? super Record> pred) {
		return new FilteredRecordSet(this, pred);
	}
	
	public default FlatTransformedRecordSet flatMap(RecordSchema outSchema,
													Function<? super Record,RecordSet> transform) {
		return new FlatTransformedRecordSet(this, outSchema, transform);
	}
	
	public default <S> S foldLeft(S accum, S stopper,
								BiFunction<? super S,? super Record,? extends S> folder) {
		Utilities.checkNotNullArgument(folder, "folder is null");

		try {
			if ( accum.equals(stopper) ) {
				return accum;
			}
	
			Record record = DefaultRecord.of(getRecordSchema());
			while ( next(record) ) {
				accum = folder.apply(accum, record);
				if ( accum.equals(stopper) ) {
					return accum;
				}
			}
			
			return accum;
		}
		finally {
			closeQuietly();
		}
	}
	
	public default <S> S foldLeft(S accum,
								BiFunction<? super S,? super Record,? extends S> folder) {
		Utilities.checkNotNullArgument(folder, "folder is null");

		try {
			Record record = DefaultRecord.of(getRecordSchema());
			while ( next(record) ) {
				accum = folder.apply(accum, record);
			}
			
			return accum;
		}
		finally {
			closeQuietly();
		}
	}
	
	public default <S> S collectLeft(S accum, BiConsumer<? super S,? super Record> consumer) {
		Utilities.checkNotNullArgument(consumer, "consumer is null");

		try {
			Record record = DefaultRecord.of(getRecordSchema());
			while ( next(record) ) {
				consumer.accept(accum, record);
			}
			
			return accum;
		}
		finally {
			closeQuietly();
		}
	}
	
	public default <S> S collectLeftCopy(S accum,
										BiConsumer<? super S,? super Record> consumer) {
		Utilities.checkNotNullArgument(consumer, "consumer is null");

		try {
			for ( Record rec = nextCopy(); rec != null; rec = nextCopy() ) {
				consumer.accept(accum, rec);
			}
			
			return accum;
		}
		finally {
			closeQuietly();
		}
	}
	
	/**
	 * 본 레코드 세트에 포함된 레코드에 대해 차례대로 주어진
	 * {@link Consumer#accept(Object)}를 호출한다.
	 * <p>
	 * {@link Consumer#accept(Object)} 호출 중 오류가 발생되는 경우는 무시되고,
	 * 다음 레코드로 진행된다.
	 * 
	 * @param consumer	레코드 세트에 포함된 레코드를 처리할 레코드 소비자 객체.
	 */
	public default void forEach(Consumer<? super Record> consumer) {
		Utilities.checkNotNullArgument(consumer, "consumer is null");
		
		Record record = DefaultRecord.of(getRecordSchema());
		try {
			while ( next(record) ) {
				try {
					consumer.accept(record);
				}
				catch ( Throwable e ) {
					if ( this instanceof LoggerSettable ) {
						((LoggerSettable)this).getLogger()
												.warn("fails to consume record: " + record, e);
					}
				}
			}
		}
		catch ( Throwable e ) {
			Throwables.sneakyThrow(e);
		}
		finally {
			closeQuietly();
		}
	}
	
	/**
	 * 본 레코드 세트에 포함된 레코드에 대해 차례대로 주어진
	 * {@link Consumer#accept(Object)}를 호출한다.
	 * <p>
	 * {@link Consumer#accept(Object)} 호출 중 오류가 발생되는 경우는 무시되고,
	 * 다음 레코드로 진행된다.
	 * 
	 * @param consumer	레코드 세트에 포함된 레코드를 처리할 레코드 소비자 객체.
	 */
	public default void forEachCopy(Consumer<? super Record> consumer) {
		Utilities.checkNotNullArgument(consumer, "consumer is null");
		
		Record record;
		try {
			while ( (record = nextCopy()) != null ) {
				try {
					consumer.accept(record);
				}
				catch ( Throwable e ) {
					if ( this instanceof LoggerSettable ) {
						((LoggerSettable)this).getLogger().warn("fails to consume record: " + record, e);
					}
				}
			}
		}
		catch ( Throwable e ) {
			Throwables.sneakyThrow(e);
		}
		finally {
			closeQuietly();
		}
	}
	
	public default Record findFirst() {
		try {
			return nextCopy();
		}
		finally {
			closeQuietly();
		}
	}
	
	/**
	 * 레코드 세트에 포함된 모든 레코드로 구성된 리스트를 생성한다.
	 * <p>
	 * 전체 레코드들을 읽어 리스트가 구성되면 자동으로 {@link #close()}가 호출된다.
	 * 
	 * @return	레코드 리스트.
	 */
	public default List<Record> toList() {
		return collectLeftCopy(Lists.newArrayList(), (c,r) -> c.add(r));
	}
	
	/**
	 * 본 레코드 세트에 포함된 레코드를 접근하는 순환자 ({@link Iterator})을 반환한다.
	 * 
	 * @return	레코드 세트 순환자 객체.
	 */
	public default Iterator<Record> iterator() {
		return new Iterator<Record>() {
			@Nullable private Record m_next;
			
			{
				m_next = RecordSet.this.nextCopy();
			}

			@Override
			public boolean hasNext() {
				return m_next != null;
			}

			@Override
			public Record next() {
				if ( m_next != null ) {
					Record next = m_next;
					if ( (m_next = RecordSet.this.nextCopy()) == null ) {
						 RecordSet.this.closeQuietly();
					}
					
					return next;
				}
				else {
					throw new NoSuchElementException();
				}
			}
		};
	}
	
	/**
	 * 본 레코드 세트에 포함된 레코드를 접근하는 스트림 ({@link FStream})을 반환한다.
	 * 
	 * @return	레코드 세트 스트림 객체.
	 */
	public default FStream<Record> fstream() {
		return new FStream<Record>() {
			@Override
			public void close() throws Exception {
				RecordSet.this.close();
			}

			@Override
			public FOption<Record> next() {
				return FOption.ofNullable(RecordSet.this.nextCopy());
			}
		};
	}
	
	public default Observable<Record> observe() {
		return Observable.create(new ObservableOnSubscribe<Record>() {
			@Override
			public void subscribe(ObservableEmitter<Record> emitter) throws Exception {
				try {
					Record record;
					while ( (record = RecordSet.this.nextCopy()) != null ) {
						if ( emitter.isDisposed() ) {
							return;
						}
						emitter.onNext(record);
					}
					
					if ( emitter.isDisposed() ) {
						return;
					}
					emitter.onComplete();
				}
				catch ( Throwable e ) {
					emitter.onError(e);
				}
			}
			
		});
	}
	
	public default FOption<RecordSet> asNonEmpty() {
		PushBackableRecordSet pushable = asPushBackable();
		
		if ( pushable.hasNext() ) {
			return FOption.of(pushable);
		}
		else {
			return FOption.empty();
		}
	}
	
	public default PushBackableRecordSet asPushBackable() {
		return (this instanceof PushBackableRecordSet)
				? (PushBackableRecordSet)this
				: new PushBackableRecordSetImpl(this);
	}
	
	public default CountingRecordSet asCountingRecordSet() {
		return new CountingRecordSet(this);
	}
	
	public default ProgressReportingRecordSet reportProgress(Observer<Long> observer) {
		return new ProgressReportingRecordSet(this, observer);
	}
	@SuppressWarnings("resource")
	public default ProgressReportingRecordSet reportProgress(Observer<Long> observer, long interval) {
		return new ProgressReportingRecordSet(this, observer).reportInterval(interval);
	}
	
	public default long count() {
		Record record = DefaultRecord.of(getRecordSchema());
		
		try {
			long cnt = 0;
			while ( next(record) ) {
				++cnt;
			}
			
			return cnt;
		}
		finally {
			closeQuietly();
		}
	}
	
	public default RecordSet renameColumn(String src, String tar) {
		Map<String,String> map = Maps.newHashMap();
		map.put(src, tar);
		return new RecordSets.RenamedRecordSet(this, map);
	}
	
	public static RecordSet concat(RecordSchema schema, FStream<? extends RecordSet> rsets) {
		Utilities.checkNotNullArgument(schema, "schema is null");
		Utilities.checkNotNullArgument(rsets, "rsets is null");
		
		return new FStreamConcatedRecordSet(schema, rsets);
	}
	
	public static RecordSet concat(RecordSet... rsets) {
		Utilities.checkNotNullArguments(rsets, "rsets is null");
		
		return concat(rsets[0].getRecordSchema(), FStream.of(rsets));
	}
	
	public static RecordSet concat(RecordSet rset, Record tail) {
		Utilities.checkNotNullArgument(rset, "rset is null");
		Utilities.checkNotNullArgument(tail, "tail is null");
		Utilities.checkArgument(rset.getRecordSchema().equals(tail.getRecordSchema()),
								"incompatible RecordSchema");
		
		return concat(rset, RecordSet.of(tail));
	}
	
	public static RecordSet concat(Record head, RecordSet tail) {
		Utilities.checkNotNullArgument(head, "head is null");
		Utilities.checkNotNullArgument(tail, "tails is null");
		
		return concat(RecordSet.of(head), tail);
	}
	
	public static RecordSet concat(Iterable<? extends RecordSet> rsets) {
		Utilities.checkNotNullArgument(rsets, "rsets is null");
		
		Iterator<? extends RecordSet> iter = rsets.iterator();
		if ( !iter.hasNext() ) {
			throw new IllegalArgumentException("rset is empty");
		}
		return concat(iter.next().getRecordSchema(), FStream.from(rsets));
	}
	
	public default RecordSet asAutoCloseable() {
		return new AutoClosingRecordSet(this);
	}
	
	public default RecordSet attachCloser(Consumer<? super RecordSet> closer) {
		Utilities.checkNotNullArgument(closer, "Closer");
		
		return new CloserAttachedRecordSet(this, closer);
	}
}