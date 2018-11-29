package marmot.rset;

import io.reactivex.ObservableEmitter;
import io.reactivex.ObservableOnSubscribe;
import marmot.Record;
import marmot.RecordSet;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class RecordSetSubscriber implements ObservableOnSubscribe<Record> {
	private final RecordSet m_rset;
	
	RecordSetSubscriber(RecordSet rset) {
		m_rset = rset;
	}

	@Override
	public void subscribe(ObservableEmitter<Record> emitter) throws Exception {
		try {
			Record record;
			while ( (record = m_rset.nextCopy()) != null ) {
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

}
