package nl.waredingen.graphs.neo.mapreduce;



public class SurroundingContext {

	public long id = -1L, from = -1L, to = -1L, other = -1L, prev = -1L, next = -1L;
	public String val = null;

	public SurroundingContext() {
	}

	@Override
	public String toString() {
		return id + "\t" + other + "\t" + ((from != -1L) ? from + "\t" : "") + ((to != -1L) ? to + "\t" : "") + ((val != null) ? val + "\t" : "") + next + "\t" + prev;
	}

}
