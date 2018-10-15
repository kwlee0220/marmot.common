package marmot.rset;

import io.reactivex.ObservableEmitter;
import io.reactivex.ObservableOnSubscribe;
import io.vavr.control.Option;
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
			Option<Record> orec;
			while ( (orec = m_rset.nextCopy()).isDefined() ) {
				if ( emitter.isDisposed() ) {
					return;
				}
				emitter.onNext(orec.get());
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
