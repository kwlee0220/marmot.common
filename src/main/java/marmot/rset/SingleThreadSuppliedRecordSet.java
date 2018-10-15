package marmot.rset;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.support.DefaultRecord;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SingleThreadSuppliedRecordSet extends PipedRecordSet implements Runnable {
	private static final int PIPE_LENGTH = 16;
	private final Supplier<RecordSet> m_srcSupplier;
	
	public static SingleThreadSuppliedRecordSet start(RecordSchema schema,
													Supplier<RecordSet> srcSupplier) {
		SingleThreadSuppliedRecordSet rset = from(schema, srcSupplier);
		CompletableFuture.runAsync(rset);
		
		return rset;
	}
	
	public static SingleThreadSuppliedRecordSet from(RecordSchema schema,
													Supplier<RecordSet> srcSupplier) {
		return new SingleThreadSuppliedRecordSet(schema, srcSupplier);
	}
	
	private SingleThreadSuppliedRecordSet(RecordSchema schema, Supplier<RecordSet> srcSupplier) {
		super(schema, PIPE_LENGTH);
		
		m_srcSupplier = srcSupplier;
	}

	@Override
	public void run() {
		getLogger().debug("starting: supplying SingleThreadOwnedRecord");
		try ( RecordSet src = m_srcSupplier.get() ) {
			Record record = DefaultRecord.of(getRecordSchema());
			while ( src.next(record) ) {
				if ( !supply(record.duplicate()) ) {
					break;
				}
			}
			
			endOfSupply();
			
			getLogger().debug("done: supplying SingleThreadOwnedRecord");
		}
		catch ( Exception e ) {
			getLogger().debug("failed: supplying SingleThreadOwnedRecord: cause=" + e);
			throw e;
		}
	}
}
