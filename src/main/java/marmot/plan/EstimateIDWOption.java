package marmot.plan;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import com.google.common.collect.Lists;

import marmot.proto.optor.EstimateIDWProto.EstimateIDWOptionsProto;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class EstimateIDWOption {
	public abstract void set(EstimateIDWOptionsProto.Builder builder);
	
	public static PowerOption POWER(double power) {
		return new PowerOption(power);
	}
	
	public static EstimateIDWOptionsProto toProto(EstimateIDWOption... opts) {
		return toProto(Arrays.asList(opts));
	}
	
	public static EstimateIDWOptionsProto toProto(List<EstimateIDWOption> opts) {
		Objects.requireNonNull(opts, "EstimateIDWOption are null");
		
		return FStream.of(opts)
					.collectLeft(EstimateIDWOptionsProto.newBuilder(),
								(b,o) -> o.set(b))
					.build();
	}

	public static List<EstimateIDWOption> fromProto(EstimateIDWOptionsProto proto) {
		List<EstimateIDWOption> opts = Lists.newArrayList();
		
		switch ( proto.getOptionalPowerCase()) {
			case POWER:
				opts.add(POWER(proto.getPower()));
				break;
			default:
		}
		
		return opts;
	}
	
	public static class PowerOption extends EstimateIDWOption {
		private final double m_power;
		
		private PowerOption(double power) {
			m_power = power;
		}
		
		public double get() {
			return m_power;
		}
		
		public void set(EstimateIDWOptionsProto.Builder builder) {
			builder.setPower(m_power);
		}
		
		@Override
		public String toString() {
			return String.format("power=%.2f", m_power);
		}
	}
}
