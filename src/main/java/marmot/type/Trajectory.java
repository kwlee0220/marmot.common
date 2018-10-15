package marmot.type;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Point;

import io.vavr.Lazy;
import io.vavr.control.Option;
import marmot.geo.GeoClientUtils;
import marmot.proto.TrajectoryProto;
import marmot.proto.TrajectoryProto.SampleProto;
import marmot.support.PBSerializable;
import utils.Utilities;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Trajectory implements PBSerializable<TrajectoryProto> {
	private final ImmutableList<Sample> m_samples;
	private Lazy<LineString> m_line;
	
	public Trajectory(List<Sample> samples) {
		m_samples = ImmutableList.copyOf(samples);
		m_line = Lazy.of(() -> {
			Coordinate[] coords = m_samples.stream()
											.map(s -> new Coordinate(s.m_x, s.m_y))
											.toArray(sz -> new Coordinate[sz]);
			return (coords.length >= 2)
					? GeoClientUtils.GEOM_FACT.createLineString(coords)
					: null;
		});
	}
	
	public LineString getLineString() {
		return m_line.get();
	}
	
	public Point getStartPoint() {
		return getFirstSample().map(Sample::getPoint)
								.getOrElse(GeoClientUtils.EMPTY_POINT);
	}
	
	public Point getEndPoint() {
		return getLastSample().map(Sample::getPoint)
							.getOrElse(GeoClientUtils.EMPTY_POINT);
	}
	
	public int getLength() {
		return m_samples.size();
	}
	
	public Interval getInterval() {
		long end = m_samples.get(m_samples.size()-1).m_ts;
		long begin = m_samples.get(0).m_ts;
		
		return Interval.between(begin, end);
	}
	
	public Duration getDuration() {
		if ( m_samples.size() < 2 ) {
			return Duration.ofMillis(0);
		}
		else {
			Sample first = m_samples.get(0);
			Sample last = m_samples.get(m_samples.size()-1);
			
			return Duration.ofMillis(last.getMillis() - first.getMillis());
		}
	}
	
	public long getStartTimeMillis() {
		return getFirstSample().map(Sample::getMillis).getOrElse(-1L);
	}
	
	public long getEndTimeMillis() {
		return getLastSample().map(Sample::getMillis).getOrElse(-1L);
	}
	
	public int getSampleCount() {
		return m_samples.size();
	}
	
	public Sample getSample(int idx) {
		return m_samples.get(idx);
	}
	
	public Option<Sample> getFirstSample() {
		return m_samples.isEmpty() ? Option.none() : Option.some(m_samples.get(0));
	}
	
	public Option<Sample> getLastSample() {
		int nsamples = m_samples.size(); 
		return nsamples > 0 ? Option.some(m_samples.get(nsamples-1)) : Option.none();
	}
	
	public ImmutableList<Sample> getSampleAll() {
		return m_samples;
	}
	
	@Override
	public String toString() {
		return String.format("nsamples=%d, duration=%s", m_samples.size(), getDuration());
	}
	
	public static Trajectory fromProto(TrajectoryProto proto) {
		List<Sample> samples = FStream.of(proto.getSampleList())
												.map(Sample::fromProto)
												.toList();
		return new Trajectory(samples);
	}

	@Override
	public TrajectoryProto toProto() {
		return FStream.of(m_samples)
					.map(Sample::toProto)
					.foldLeft(TrajectoryProto.newBuilder(), (b,s) -> b.addSample(s))
					.build();
	}
	
	public static final class Sample implements PBSerializable<SampleProto> {
		public final double m_x;
		public final double m_y;
		public final long m_ts;
		
		public Sample(double x, double y, long ts) {
			m_x = x;
			m_y = y;
			m_ts = ts;
		}
		
		public Sample(Point pt, long ts) {
			m_x = pt.getX();
			m_y = pt.getY();
			m_ts = ts;
		}
		
		public Point getPoint() {
			return GeoClientUtils.toPoint(m_x, m_y);
		}
		
		public long getMillis() {
			return m_ts;
		}
		
		public double distance(Sample dst) {
			return getPoint().distance(dst.getPoint());
		}
		
		public LocalDateTime getTimestamp() {
			return Utilities.fromUTCEpocMillis(m_ts, ZoneId.systemDefault());
		}
		
		public static Sample parse(String str) {
			String[] parts = str.split(":");
			return new Sample(Double.parseDouble(parts[0]), Double.parseDouble(parts[1]),
								Long.parseLong(parts[2]));
		}
		
		@Override
		public String toString() {
			return String.format("%f:%f:%d", m_x, m_y, m_ts);
		}
		
		public static Sample fromProto(SampleProto proto) {
			return new Sample(proto.getX(), proto.getY(), proto.getTs());
		}

		@Override
		public SampleProto toProto() {
			return SampleProto.newBuilder()
								.setX(m_x)
								.setY(m_y)
								.setTs(m_ts)
								.build();
		}
	}
	
	public static Builder builder() {
		return new Builder();
	}
	public static class Builder {
		private final List<Sample> m_samples = Lists.newArrayList();
		
		public void clear() {
			m_samples.clear();
		}
		
		public int length() {
			return m_samples.size();
		}
		
		public Builder add(Sample sample) {
			m_samples.add(sample);
			return this;
		}
		
		public Builder add(Point pt, long ts) {
			return add(new Sample(pt.getX(), pt.getY(), ts));
		}
		
		public Trajectory build() {
			return new Trajectory(m_samples);
		}
		
		public Option<Duration> getDuration() {
			if ( m_samples.size() < 2 ) {
				return Option.none();
			}
			else {
				Sample first = m_samples.get(0);
				Sample last = m_samples.get(m_samples.size()-1);
				
				return Option.some(Duration.ofMillis(last.getMillis() - first.getMillis()));
			}
		}
		
		@Override
		public String toString() {
			String durStr = getDuration()
								.map(dur -> ", duration=" + dur)
								.getOrElse("");
			return String.format("nsamples=%d%s", m_samples.size(), durStr);
		}
	}
}
