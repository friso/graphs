package nl.waredingen.graphs.neo.mapreduce.edges.surround;


public class EdgeSurroundContext {

	long id = -1L, from = -1L, to = -1L, relnum = -1L, prev = -1L, next = -1L;

	public EdgeSurroundContext() {
	}

	@Override
	public String toString() {
		return id + "\t" + relnum + "\t" + from + "\t" + to + "\t" + next + "\t" + prev;
	}

}
