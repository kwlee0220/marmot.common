package marmot.type;

import java.io.Serializable;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;

import com.google.common.collect.Lists;

import utils.LocalDateTimes;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Interval implements Serializable {
	private static final long serialVersionUID = -3126854516768848835L;

	public static final Interval EMPTY = new Interval(-1, -1);
	
	private transient final org.joda.time.Interval m_jodaInterval;
	
	public static Interval between(LocalDateTime start, LocalDateTime end) {
		return between(LocalDateTimes.toUtcMillis(start), LocalDateTimes.toUtcMillis(end));
	}
	
	public static Interval between(long start, long end) {
		return new Interval(start, end);
	}
	
	private Interval(long start, long end) {
		m_jodaInterval = new org.joda.time.Interval(start, end);
	}
	
	private Interval(org.joda.time.Interval jodaInterval) {
		m_jodaInterval = jodaInterval;
	}
	
	public long getStartMillis() {
		return m_jodaInterval.getStartMillis();
	}
	
	public long getEndMillis() {
		return m_jodaInterval.getEndMillis();
	}
	
	public boolean contains(long millis) {
		return m_jodaInterval.contains(millis);
	}
	
	public boolean overlaps(Interval interval) {
		return m_jodaInterval.overlaps(interval.m_jodaInterval);
	}
	
	public Interval overlap(Interval interval) {
		return new Interval(m_jodaInterval.overlap(interval.m_jodaInterval));
	}
	
	public Duration toDuration() {
		return Duration.ofMillis(m_jodaInterval.toDurationMillis());
	}
	
	public long toDurationMillis() {
		return m_jodaInterval.toDurationMillis();
	}
	
	public long distance(long ts) {
		if ( ts <= getStartMillis() ) {
			return ts - getStartMillis();
		}
		else if ( ts >= getEndMillis() ) {
			return ts - getEndMillis();
		}
		else {
			return 0;
		}
	}
	
	public long distance(Interval intvl) {
		if ( intvl.getEndMillis() <= getStartMillis() ) {
			return intvl.getEndMillis() - getStartMillis();
		}
		else if ( intvl.getStartMillis() >= getEndMillis() ) {
			return intvl.getStartMillis() - getEndMillis();
		}
		else {
			return 0;
		}
	}
	
	public static Interval merge(Interval intvl1, Interval intvl2) {
		return Interval.between(Math.min(intvl1.getStartMillis(), intvl2.getStartMillis()),
								Math.max(intvl1.getEndMillis(), intvl2.getEndMillis()));
	}
	
	@Override
	public String toString() {
		return String.format("%s[%s]", m_jodaInterval, toDuration());
	}
	
	public static List<Interval> cluster(List<Long> points, long threshold) {
		List<Interval> intervals = FStream.from(points)
											.map(pt -> Interval.between(pt, pt))
											.toList();

		final List<Interval> clusters = Lists.newArrayList();
		for ( Interval intvl: intervals ) {
			List<Interval> neighbors
						= FStream.from(clusters)
								.filter(c -> Math.abs(c.distance(intvl)) <= threshold)
								.toList();
			clusters.removeAll(neighbors);
			Interval merged = FStream.from(neighbors)
									.fold(intvl, (i,c) -> Interval.merge(i, c));
			clusters.add(merged);
		}
		
		return clusters;
	}
}
