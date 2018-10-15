package marmot.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.stream.Stream;

import marmot.type.Trajectory.Sample;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TrajectoryType extends DataType {
	private static final TrajectoryType TYPE = new TrajectoryType();
	
	public static TrajectoryType get() {
		return TYPE;
	}
	
	private TrajectoryType() {
		super("trajectory", TypeCode.TRAJECTORY, Trajectory.class);
	}

	@Override
	public Trajectory newInstance() {
		return Trajectory.builder().build();
	}
	
	@Override
	public Trajectory fromString(String str) {
		Trajectory.Builder builder = Trajectory.builder();
		Stream.of(str.split(","))
				.map(Sample::parse)
				.forEach(s -> builder.add(s));

		return builder.build();
	}

	@Override
	public Trajectory readObject(DataInput in) throws IOException {
		int nsamples = in.readInt();
		
		Trajectory.Builder builder = Trajectory.builder();
		for ( int i =0; i < nsamples; ++i ) {
			double x = in.readDouble();
			double y = in.readDouble();
			long ts = in.readLong();
			
			builder.add(new Sample(x, y, ts));
		}
		
		return builder.build();
	}

	@Override
	public void writeObject(Object obj, DataOutput out) throws IOException {
		Trajectory traj = (Trajectory)obj;
		
		out.writeInt(traj.getSampleCount());
		for ( Sample sample: traj.getSampleAll() ) {
			out.writeDouble(sample.m_x);
			out.writeDouble(sample.m_y);
			out.writeLong(sample.m_ts);
		}
	}
}
