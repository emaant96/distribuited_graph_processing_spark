package SparkProject.graphWork;

import java.util.Comparator;

class comp1 implements Comparator<Double>, Comparable<Double> {

	@Override
	public int compareTo(Double o) {
		// TODO Auto-generated method stub
		return 0;
	}
	@Override
	public int compare(Double d, Double d1) {
	      return (int)(d - d1);
	   }
	}
