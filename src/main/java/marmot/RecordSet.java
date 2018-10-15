package marmot;

import java.io.Closeable;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.reactivex.Observer;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.control.Option;
import io.vavr.control.Try;
import marmot.rset.RecordSetIterator;
import marmot.support.DefaultRecord;
import utils.LoggerSettable;
import utils.Throwables;
import utils.Utilities;
import utils.stream.FStream;
import utils.stream.FStreamImpl;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface RecordSet extends Closeable {
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
	
	public default Try<Void> closeQuietly() {
		return Try.run(this::close);
	}
	
	/**
	 * 레코드 세트의 다음번 레코드를 읽어 주어진 레코드에 적재시킨다.
	 * 
	 * @param output	다음 레코드가 저장될 객체.
	 * @return	적재 여부. 레코드 세트에 더 이상의 레코드가 없는 경우는 false
	 */
	public default boolean next(Record output) {
		return nextCopy()
				.map(copy -> { 
					output.set(copy, true);
					return true;
				})
				.getOrElse(false);
	}
	
	/**
	 * 레코드 세트의 다음번 레코드를 읽어 반환한다.
	 * 
	 * @return	읽은 레코드 객체.  레코드가 없는 경우는 {@link Option#none()}이 반환됨.
	 */
	public default Option<Record> nextCopy() {
		Record output = DefaultRecord.of(getRecordSchema());
		return ( next(output) ) ? Option.some(output) : Option.none();
	}
	
	public default <S> S foldLeft(S accum, S stopper,
									BiFunction<? super S,? super Record,? extends S> folder) {
		Preconditions.checkArgument(accum != null, "accum is null");
		Preconditions.checkArgument(folder != null, "folder is null");
		
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
	
	public default <S> S foldLeft(S accum, BiFunction<? super S,? super Record,? extends S> folder) {
		Preconditions.checkArgument(accum != null, "accum is null");
		Preconditions.checkArgument(folder != null, "folder is null");

		Record record = DefaultRecord.of(getRecordSchema());
		while ( next(record) ) {
			accum = folder.apply(accum, record);
		}
		
		return accum;
	}
	
	public default <S> S foldLeftCopy(S accum,
									BiFunction<? super S,? super Record,? extends S> folder) {
		Preconditions.checkArgument(accum != null, "accum is null");
		Preconditions.checkArgument(folder != null, "folder is null");

		Option<Record> orec;
		while ( (orec = nextCopy()).isDefined() ) {
			accum = folder.apply(accum, orec.get());
		}
		
		return accum;
	}
	
	public default <S> S collectLeft(S accum, BiConsumer<? super S,? super Record> consumer) {
		Objects.requireNonNull(accum);
		Objects.requireNonNull(consumer);

		Record record = DefaultRecord.of(getRecordSchema());
		while ( next(record) ) {
			consumer.accept(accum, record);
		}
		
		return accum;
	}
	
	public default <S> S collectLeftCopy(S accum, BiConsumer<? super S,? super Record> consumer) {
		Objects.requireNonNull(accum);
		Objects.requireNonNull(consumer);

		Option<Record> orec;
		while ( (orec = nextCopy()).isDefined() ) {
			consumer.accept(accum, orec.get());
		}
		
		return accum;
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
		forEach(consumer, null);
	}
	
	/**
	 * 본 레코드 세트에 포함된 레코드에 대해 차례대로 주어진
	 * {@link Consumer#accept(Object)}를 호출한다.
	 * <p>
	 * {@link Consumer#accept(Object)} 호출 중 오류가 발생되는 경우는 무시되고,
	 * 다음 레코드로 진행된다.
	 * 
	 * @param consumer	레코드 세트에 포함된 레코드를 처리할 레코드 소비자 객체.
	 * @param failObserver	레코드 처리 중 오류 발생시 이를 report할 observer.
	 * 					별도로 지정하지 않을 경우는 {@code null}을 제공.
	 */
	public default void forEach(Consumer<? super Record> consumer,
								Observer<Tuple2<Record,Throwable>> failObserver) {
		Record record = DefaultRecord.of(getRecordSchema());
		try {
			while ( next(record) ) {
				try {
					consumer.accept(record);
				}
				catch ( Throwable e ) {
					if ( failObserver != null ) {
						failObserver.onNext(Tuple.of(record.duplicate(), e));
					}
					else if ( this instanceof LoggerSettable ) {
						((LoggerSettable)this).getLogger().warn("fails to consume record: " + record, e);
					}
				}
			}
		}
		catch ( Throwable e ) {
			Throwables.throwIfInstanceOf(e, RecordSetException.class);
			throw new RecordSetException(e);
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
	 * @param failObserver	레코드 처리 중 오류 발생시 이를 report할 observer.
	 * 					별도로 지정하지 않을 경우는 {@code null}을 제공.
	 */
	public default void forEachCopy(Consumer<? super Record> consumer,
									Observer<Tuple2<Record,Throwable>> failObserver) {
		Option<Record> orecord;
		try {
			while ( (orecord = nextCopy()).isDefined() ) {
				final Record record = orecord.get();
				try {
					consumer.accept(record);
				}
				catch ( Throwable e ) {
					if ( failObserver != null ) {
						failObserver.onNext(Tuple.of(record, e));
					}
					else if ( this instanceof LoggerSettable ) {
						((LoggerSettable)this).getLogger().warn("fails to consume record: " + record, e);
					}
				}
			}
		}
		catch ( Throwable e ) {
			Throwables.throwIfInstanceOf(e, RecordSetException.class);
			throw new RecordSetException(e);
		}
	}
	
	public default void forEachCopy(Consumer<? super Record> consumer) {
		forEachCopy(consumer, null);
	}
	
	/**
	 * 레코드 세트에 포함된 모든 레코드로 구성된 리스트를 생성한다.
	 * <p>
	 * 전체 레코드들을 읽어 리스트가 구성되면 자동으로 {@link #close()}가 호출된다.
	 * 
	 * @return	레코드 리스트.
	 */
	public default List<Record> toList() {
		Option<Record> record;
		List<Record> recordList = Lists.newArrayList();
		try {
			while ( (record = nextCopy()).isDefined() ) {
				recordList.add(record.get());
			}
			
			return recordList;
		}
		finally {
			closeQuietly();
		}
	}
	
	public default Option<Record> getFirst() {
		try {
			return nextCopy();
		}
		finally {
			closeQuietly();
		}
	}
	
	/**
	 * 본 레코드 세트에 포함된 레코드를 접근하는 순환자 ({@link Iterator})을 반환한다.
	 * 
	 * @return	레코드 세트 순환자 객체.
	 */
	public default Iterator<Record> iterator() {
		return new RecordSetIterator(this);
	}
	
	/**
	 * 본 레코드 세트에 포함된 레코드를 접근하는 스트림 ({@link Stream})을 반환한다.
	 * 
	 * @return	레코드 세트 스트림 객체.
	 */
	public default Stream<Record> stream() {
		return Utilities.stream(iterator());
	}
	
	public default FStream<Record> fstream() {
		return new FStreamImpl<>(
			"RecordSet::fstream",
			() -> nextCopy(),
			() -> close()
		);
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
}