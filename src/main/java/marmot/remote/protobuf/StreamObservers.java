package marmot.remote.protobuf;

import io.grpc.stub.StreamObserver;
import marmot.proto.service.DownChunkRequest;
import marmot.proto.service.DownChunkResponse;
import marmot.proto.service.ErrorProto;
import marmot.proto.service.UpChunkRequest;
import marmot.proto.service.UpChunkResponse;
import marmot.proto.service.UpRequestDownResponse;
import marmot.proto.service.UpResponseDownRequest;
import marmot.protobuf.PBUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class StreamObservers {
	private StreamObservers() {
		throw new AssertionError("Should not be called: class=" + StreamObservers.class);
	}

	public static ClientUpDownChannel getClientUpDownChannel(
												StreamObserver<UpRequestDownResponse> channel) {
		return new ClientUpDownChannel(channel);
	}

	public static ServerUpDownChannel getServerUpDownChannel(
												StreamObserver<UpResponseDownRequest> channel) {
		return new ServerUpDownChannel(channel);
	}
	
	public static class ClientUpDownChannel {
		private final StreamObserver<UpRequestDownResponse> m_channel;
		private boolean m_upChannelClosed = false;
		private boolean m_downChannelClosed = false;
		
		ClientUpDownChannel(StreamObserver<UpRequestDownResponse> channel) {
			m_channel = channel;
		}
		
		public StreamObserver<UpChunkRequest> getUploadChannel() {
			return new UploadChannel();
		}
		
		public StreamObserver<DownChunkResponse> getDownloadChannel() {
			return new DownloadChannel();
		}
		
		private class UploadChannel implements StreamObserver<UpChunkRequest> {
			@Override
			public void onNext(UpChunkRequest req) {
				UpRequestDownResponse msg = UpRequestDownResponse.newBuilder()
																	.setUpReq(req)
																	.build();
				synchronized ( m_channel ) {
					m_channel.onNext(msg);
				}
			}

			@Override
			public void onError(Throwable cause) {
				ErrorProto error = PBUtils.toErrorProto(cause);
				UpRequestDownResponse msg = UpRequestDownResponse.newBuilder()
																.setUpError(error)
																.build();
				synchronized ( m_channel ) {
					m_channel.onNext(msg);
				}
			}

			@Override
			public void onCompleted() {
				UpRequestDownResponse msg = UpRequestDownResponse.newBuilder()
																.setUpClosed(PBUtils.VOID)
																.build();
				synchronized ( m_channel ) {
					m_channel.onNext(msg);
					m_upChannelClosed = false;
					if ( m_downChannelClosed ) {
						m_channel.onCompleted();
					}
				}
			}
		}
		
		private class DownloadChannel implements StreamObserver<DownChunkResponse> {
			@Override
			public void onNext(DownChunkResponse resp) {
				UpRequestDownResponse msg = UpRequestDownResponse.newBuilder()
																.setDownResp(resp)
																.build();
				synchronized ( m_channel ) {
					m_channel.onNext(msg);
				}
			}

			@Override
			public void onError(Throwable cause) {
				ErrorProto error = PBUtils.toErrorProto(cause);
				UpRequestDownResponse msg = UpRequestDownResponse.newBuilder()
																.setDownError(error)
																.build();
				synchronized ( m_channel ) {
					m_channel.onNext(msg);
				}
			}

			@Override
			public void onCompleted() {
				UpRequestDownResponse msg = UpRequestDownResponse.newBuilder()
																.setDownClosed(PBUtils.VOID)
																.build();
				synchronized ( m_channel ) {
					m_channel.onNext(msg);
					m_downChannelClosed = false;
					if ( m_upChannelClosed ) {
						m_channel.onCompleted();
					}
				}
			}
		}
	}
	
	public static class ServerUpDownChannel {
		private final StreamObserver<UpResponseDownRequest> m_channel;
		private boolean m_upChannelClosed = false;
		private boolean m_downChannelClosed = false;
		
		ServerUpDownChannel(StreamObserver<UpResponseDownRequest> channel) {
			m_channel = channel;
		}
		
		public StreamObserver<UpChunkResponse> getUploadChannel() {
			return new UploadChannel();
		}
		
		public StreamObserver<DownChunkRequest> getDownloadChannel() {
			return new DownloadChannel();
		}
		
		private class UploadChannel implements StreamObserver<UpChunkResponse> {
			@Override
			public void onNext(UpChunkResponse resp) {
				synchronized ( m_channel ) {
					m_channel.onNext(UpResponseDownRequest.newBuilder()
															.setUpResp(resp)
															.build());
				}
			}

			@Override
			public void onError(Throwable cause) {
				ErrorProto error = PBUtils.toErrorProto(cause);
				synchronized ( m_channel ) {
					m_channel.onNext(UpResponseDownRequest.newBuilder()
															.setUpError(error)
															.build());
				}
			}

			@Override
			public void onCompleted() {
				synchronized ( m_channel ) {
					if ( !m_upChannelClosed ) {
						if ( m_downChannelClosed ) {
							m_channel.onCompleted();
						}
						else {
							m_channel.onNext(UpResponseDownRequest.newBuilder()
									.setUpClosed(PBUtils.VOID)
									.build());
						}
						m_upChannelClosed = true;
					}
				}
			}
		}
		
		private class DownloadChannel implements StreamObserver<DownChunkRequest> {
			@Override
			public void onNext(DownChunkRequest resp) {
				synchronized ( m_channel ) {
					m_channel.onNext(UpResponseDownRequest.newBuilder().setDownReq(resp).build());
				}
			}

			@Override
			public void onError(Throwable cause) {
				ErrorProto error = PBUtils.toErrorProto(cause);
				synchronized ( m_channel ) {
					m_channel.onNext(UpResponseDownRequest.newBuilder().setDownError(error).build());
				}
			}

			@Override
			public void onCompleted() {
				synchronized ( m_channel ) {
					if ( !m_downChannelClosed ) {
						if ( m_upChannelClosed ) {
							m_channel.onCompleted();
						}
						else {
							m_channel.onNext(UpResponseDownRequest.newBuilder()
																	.setDownClosed(PBUtils.VOID)
																	.build());
						}
						m_downChannelClosed = true;
					}
				}
			}
		}
	}
}
