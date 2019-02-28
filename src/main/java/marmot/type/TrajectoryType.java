package marmot.type;

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
	public Trajectory parseInstance(String str) {
		Trajectory.Builder builder = Trajectory.builder();
		Stream.of(str.split(","))
				.map(Sample::parse)
				.forEach(s -> builder.add(s));

		return builder.build();
	}
}
