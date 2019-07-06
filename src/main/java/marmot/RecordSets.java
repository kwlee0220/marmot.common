package marmot;

import java.util.Iterator;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

import com.google.common.collect.Maps;

import marmot.rset.AbstractRecordSet;
import marmot.rset.PeekableRecordSet;
import utils.Utilities;
import utils.io.IOUtils;
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
	
	static class EmptyRecordSet implements RecordSet {
		private final RecordSchema m_schema;
		
		EmptyRecordSet(RecordSchema schema) {
			m_schema = schema;
		}
		
		@Override
		public void close() { }

		@Override
		public RecordSchema getRecordSchema() {
			return m_schema;
		}
		
		@Override
		public boolean next(Record output) {
			return false;
		}

		@Override
		public Record nextCopy() {
			return null;
		}
	}
	
	static class FStreamRecordSet extends AbstractRecordSet {
		private final RecordSchema m_schema;
		private final FStream<? extends Record> m_stream;
		
		FStreamRecordSet(RecordSchema schema, FStream<? extends Record> stream) {
			m_schema = schema;
			m_stream = stream;
		}
		
		@Override
		public RecordSchema getRecordSchema() {
			return m_schema;
		}
		
		@Override
		protected void closeInGuard() throws Exception {
			m_stream.closeQuietly();
		}
		
		@Override
		public Record nextCopy() {
			checkNotClosed();
			
			return m_stream.next().getOrNull();
		}
	}
	
	static class IteratorRecordSet extends AbstractRecordSet {
		private final RecordSchema m_schema;
		private final Iterator<? extends Record> m_iter;
		
		IteratorRecordSet(RecordSchema schema, Iterator<? extends Record> iter) {
			m_schema = schema;
			m_iter = iter;
		}
		
		@Override
		protected void closeInGuard() {
			IOUtils.closeQuietly(m_iter);
		}

		@Override
		public RecordSchema getRecordSchema() {
			return m_schema;
		}
		
		@Override
		public Record nextCopy() {
			checkNotClosed();
			
			return (m_iter.hasNext()) ? m_iter.next() : null;
		}
	}
	
	static class LazyRecordSet extends AbstractRecordSet {
		private final RecordSchema m_schema;
		private final Supplier<RecordSet> m_supplier;
		private RecordSet m_lazy = null;
		
		LazyRecordSet(RecordSchema schema, Supplier<RecordSet> supplier) {
			m_schema = schema;
			m_supplier = supplier;
		}

		@Override
		protected void closeInGuard() {
			if ( m_lazy != null ) {
				m_lazy.closeQuietly();
			}
		}

		@Override
		public RecordSchema getRecordSchema() {
			return m_schema;
		}

		@Override
		public boolean next(Record output) {
			if ( m_lazy == null ) {
				m_lazy = m_supplier.get();
			}
			
			return m_lazy.next(output);
		}

		@Override
		public Record nextCopy() {
			if ( m_lazy == null ) {
				m_lazy = m_supplier.get();
			}
			
			return m_lazy.nextCopy();
		}
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
	
	static class FilteredRecordSet extends AbstractRecordSet {
		private final RecordSet m_rset;
		private final Predicate<? super Record> m_pred;
		
		FilteredRecordSet(RecordSet rset, Predicate<? super Record> pred) {
			m_rset = rset;
			m_pred = pred;
		}
		
		@Override
		public RecordSchema getRecordSchema() {
			return m_rset.getRecordSchema();
		}

		@Override
		protected void closeInGuard() throws Exception {
			m_rset.closeQuietly();
		}
		
		@Override
		public boolean next(Record output) {
			while ( next(output) ) {
				if ( m_pred.test(output) ) {
					return true;
				}
			}
			
			return false;
		}
		
		@Override
		public Record nextCopy() {
			Record rec;
			while ( (rec = nextCopy()) != null ) {
				if ( m_pred.test(rec) ) {
					return rec;
				}
			}
			
			return null;
		}
		
	}
	
	public static class CountingRecordSet extends AbstractRecordSet {
		private final RecordSet m_rset;
		private long m_count = 0;
		
		CountingRecordSet(RecordSet rset) {
			Utilities.checkNotNullArgument(rset, "input RecordSet is null");
			
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
	
	static class RenamedRecordSet extends AbstractRecordSet {
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
	
	static class AutoClosingRecordSet extends AbstractRecordSet {
		private final RecordSet m_rset;
		
		AutoClosingRecordSet(RecordSet rset) {
			m_rset = rset;
		}
		
		@Override
		public void closeInGuard() {
			m_rset.close();
		}

		@Override
		public RecordSchema getRecordSchema() {
			return m_rset.getRecordSchema();
		}
		
		@Override
		public boolean next(Record record) {
			boolean done = m_rset.next(record);
			if ( !done ) {
				closeQuietly();
			}
			
			return done;
		}
		
		@Override
		public Record nextCopy() {
			Record next = m_rset.nextCopy();
			if ( next == null ) {
				closeQuietly();
				return null;
			}
			else {
				return next;
			}
		}
	}
	
	static class CloserAttachedRecordSet extends AbstractRecordSet {
		private final RecordSet m_rset;
		private final Consumer<? super RecordSet> m_closer;
		
		CloserAttachedRecordSet(RecordSet rset, Consumer<? super RecordSet> closer) {
			m_rset = rset;
			m_closer = closer;
		}
		
		@Override
		protected void closeInGuard() {
			m_rset.closeQuietly();
			
			synchronized ( this ) {
				m_closer.accept(this);
			}
		}

		@Override
		public RecordSchema getRecordSchema() {
			return m_rset.getRecordSchema();
		}
		
		@Override
		public boolean next(Record record) {
			return m_rset.next(record);
		}
		
		@Override
		public Record nextCopy() {
			return m_rset.nextCopy();
		}
		
	}
}
