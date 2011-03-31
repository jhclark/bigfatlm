package bigfat.util;

public class Sanity {
	
	public static void checkValidReal(double real) {
		if(Double.isInfinite(real) || Double.isNaN(real)) {
			throw new IllegalArgumentException("Not a valid real number: " + real);
		}
	}
	
	public static void checkValidReal(double real, String msgFormat,
			Object... args) {
		
		if(Double.isInfinite(real) || Double.isNaN(real)) {
			throw new IllegalArgumentException("Not a valid real number: " + real + " :: " + String.format(msgFormat, args));
		}
	}
	
	public static void checkValidLogProb(double logProb) {
		checkValidReal(logProb);
		if(logProb > 0.0) {
			throw new IllegalArgumentException("Not a valid log probability: " + logProb);
		}
	}
	
	public static void checkValidLogProb(double logProb, String msgFormat,
			Object... args) {
		
		checkValidReal(logProb, msgFormat, args);
		if(logProb > 0.0) {
			throw new IllegalArgumentException("Not a valid log probability: " + logProb + " :: " + String.format(msgFormat, args));
		}
	}
	
	public static void checkValidProb(double prob) {
		checkValidReal(prob);
		if(prob < 0.0 || prob > 1.0) {
			throw new IllegalArgumentException("Not a valid probability: " + prob);
		}
	}
	
	public static void checkValidProb(double prob, String msgFormat,
			Object... args) {
		
		checkValidReal(prob, msgFormat, args);
		if(prob < 0.0 || prob > 1.0) {
			throw new IllegalArgumentException("Not a valid probability: " + prob + " :: " + String.format(msgFormat, args));
		}
	}
	
	public static void checkValidBackoff(double bo) {
		checkValidReal(bo);
		if(bo < 0.0) {
			throw new IllegalArgumentException("Not a valid backoff weight: " + bo);
		}
	}
	
	public static void checkValidBackoff(double bo, String msgFormat, Object... args) {
		checkValidReal(bo, msgFormat, args);
		if(bo < 0.0) {
			throw new IllegalArgumentException("Not a valid backoff weight: " + bo + " :: " + String.format(msgFormat, args));
		}
	}
}
