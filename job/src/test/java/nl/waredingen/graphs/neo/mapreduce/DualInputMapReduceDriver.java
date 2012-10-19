package nl.waredingen.graphs.neo.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.MapReduceDriverBase;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;

//import com.google.common.collect.Lists;

/**
 * Extension to MapReduceDriver which allows you to test as if you were using
 * MultipleInputs with two input streams and two mappers. Useful for testing
 * reduce-side joins.
 * 
 * @param <KIN1>
 *            Type of input key of mapper 1
 * @param <VIN1>
 *            Type of input value of mapper 1
 * @param <KIN2>
 *            Type of input key of mapper 2
 * @param <VIN2>
 *            Type of input value of mapper 2
 * @param <KMAP>
 *            Type of output key of both mappers, e.g. if we are doing a join
 *            the join key.
 * @param <VMAP>
 *            Type of output value of both mappers, e.g. a common base class of
 *            VIN1 and VIN2.
 * @param <KOUT>
 *            Type of output key from the reducer.
 * @param <VOUT>
 *            Type of output value from the reducer.
 * 
 * @author Jacob Metcalf
 */

@SuppressWarnings("rawtypes")
public class DualInputMapReduceDriver<KIN1, VIN1, KIN2, VIN2, KMAP extends Comparable, VMAP, KOUT, VOUT> extends
		MapReduceDriverBase<KIN1, VIN1, KMAP, VMAP, KOUT, VOUT> {

	protected Mapper<KIN1, VIN1, KMAP, VMAP> firstMapper;
	protected Mapper<KIN2, VIN2, KMAP, VMAP> secondMapper;
	protected Reducer<KMAP, VMAP, KMAP, VMAP> combiner;
	protected Reducer<KMAP, VMAP, KOUT, VOUT> reducer;
	protected Counters counters = new Counters();
	@SuppressWarnings("unchecked")
	protected List<Pair<KIN2, VIN2>> secondInputList = new ArrayList();

	/**
	 * Set the First Mapper instance to use with this test driver
	 * 
	 * @param m
	 *            the Mapper instance to use
	 */
	public void setFirstMapper(final Mapper<KIN1, VIN1, KMAP, VMAP> mapper) {
		firstMapper = mapper;
	}

	/**
	 * Sets the First Mapper instance to use and returns self for fluent style
	 * */
	public DualInputMapReduceDriver<KIN1, VIN1, KIN2, VIN2, KMAP, VMAP, KOUT, VOUT> withFirstMapper(
			final Mapper<KIN1, VIN1, KMAP, VMAP> mapper) {
		setFirstMapper(mapper);
		return this;
	}

	/**
	 * @return the First Mapper object being used by this test
	 */
	public Mapper<KIN1, VIN1, KMAP, VMAP> getFirstMapper() {
		return firstMapper;
	}

	/**
	 * Set the Second Mapper instance to use with this test driver
	 * 
	 * @param m
	 *            the Second Mapper instance to use
	 */
	public void setSecondMapper(final Mapper<KIN2, VIN2, KMAP, VMAP> mapper) {
		secondMapper = mapper;
	}

	/**
	 * Sets the Second Mapper instance to use and returns self for fluent style
	 * */
	public DualInputMapReduceDriver<KIN1, VIN1, KIN2, VIN2, KMAP, VMAP, KOUT, VOUT> withSecondMapper(
			final Mapper<KIN2, VIN2, KMAP, VMAP> mapper) {
		setSecondMapper(mapper);
		return this;
	}

	/**
	 * @return the Second Mapper object being used by this test
	 */
	public Mapper<KIN2, VIN2, KMAP, VMAP> getSecondMapper() {
		return secondMapper;
	}

	/**
	 * Sets the reducer object to use for this test. Since the input object
	 * could be VMAP1 or VMAP2 we will have to specify Object.
	 * 
	 * @param r
	 *            The reducer object to use
	 */
	public void setReducer(final Reducer<KMAP, VMAP, KOUT, VOUT> r) {
		reducer = r;
	}

	/**
	 * Identical to setReducer(), but with fluent programming style
	 * 
	 * @param r
	 *            The Reducer to use
	 * @return this
	 */
	public DualInputMapReduceDriver<KIN1, VIN1, KIN2, VIN2, KMAP, VMAP, KOUT, VOUT> withReducer(
			final Reducer<KMAP, VMAP, KOUT, VOUT> r) {
		setReducer(r);
		return this;
	}

	/**
	 * @return the Reducer object being used for this test
	 */
	public Reducer<KMAP, VMAP, KOUT, VOUT> getReducer() {
		return reducer;
	}

	/**
	 * Sets the reducer object to use as a combiner for this test
	 * 
	 * @param c
	 *            The combiner object to use
	 */
	public void setCombiner(final Reducer<KMAP, VMAP, KMAP, VMAP> c) {
		combiner = c;
	}

	/**
	 * Identical to setCombiner(), but with fluent programming style
	 * 
	 * @param c
	 *            The Combiner to use
	 * @return this
	 */
	public DualInputMapReduceDriver<KIN1, VIN1, KIN2, VIN2, KMAP, VMAP, KOUT, VOUT> withCombiner(
			final Reducer<KMAP, VMAP, KMAP, VMAP> c) {
		setCombiner(c);
		return this;
	}

	/**
	 * @return the Combiner object being used for this test
	 */
	public Reducer<KMAP, VMAP, KMAP, VMAP> getCombiner() {
		return combiner;
	}

	/**
	 * Identical to setGroupingCompataor(), but with fluent programming style
	 * 
	 * @param g
	 *            The grouping comparator to use
	 * @return this
	 */
	public DualInputMapReduceDriver<KIN1, VIN1, KIN2, VIN2, KMAP, VMAP, KOUT, VOUT> withKeyGroupingComparator(
			final RawComparator<KMAP> g) {
		setKeyGroupingComparator(g);
		return this;
	}

	/**
	 * @return the Grouping Comparator being used for the shuffle on this test
	 */
	public Comparator<KMAP> getGroupingComparator() {
		return keyGroupComparator;
	}

	/**
	 * Identical to setKeyOrderCompataor(), but with fluent programming style
	 * 
	 * @param s
	 *            The key order comparator to use
	 * @return this
	 */
	public DualInputMapReduceDriver<KIN1, VIN1, KIN2, VIN2, KMAP, VMAP, KOUT, VOUT> withKeyOrderComparator(
			final RawComparator<KMAP> s) {
		setKeyOrderComparator(s);
		return this;
	}

	/**
	 * @return the Key order comparator being used for this test
	 */
	public Comparator<KMAP> getKeyOrderComparator() {
		return keyValueOrderComparator;
	}

	/** @return the counters used in this test */
	public Counters getCounters() {
		return counters;
	}

	/**
	 * @param configuration
	 *            The configuration object that will given to the mapper and/or
	 *            reducer associated with the driver
	 */
	public DualInputMapReduceDriver<KIN1, VIN1, KIN2, VIN2, KMAP, VMAP, KOUT, VOUT> withConfiguration(
			final Configuration configuration) {
		setConfiguration(configuration);
		return this;
	}

	/**
	 * Sets the counters object to use for this test.
	 * 
	 * @param ctrs
	 *            The counters object to use.
	 */
	public void setCounters(final Counters ctrs) {
		counters = ctrs;
		//counterWrapper = new CounterWrapper(counters);
	}

	/** Sets the counters to use and returns self for fluent style */
	public DualInputMapReduceDriver<KIN1, VIN1, KIN2, VIN2, KMAP, VMAP, KOUT, VOUT> withCounters(final Counters ctrs) {
		setCounters(ctrs);
		return this;
	}

	/**
	 * Identical to addInput() but returns self for fluent programming style
	 * 
	 * @param key
	 * @param val
	 * @return this
	 */
	public DualInputMapReduceDriver<KIN1, VIN1, KIN2, VIN2, KMAP, VMAP, KOUT, VOUT> withFirstInput(final KIN1 key,
			final VIN1 val) {
		addInput(key, val);
		return this;
	}

	/**
	 * Identical to addInput() but returns self for fluent programming style
	 * 
	 * @param key
	 * @param val
	 * @return this
	 */
	public DualInputMapReduceDriver<KIN1, VIN1, KIN2, VIN2, KMAP, VMAP, KOUT, VOUT> withFirstInput(
			final Pair<KIN1, VIN1> pair) {
		addInput(pair);
		return this;
	}

	/**
	 * Adds an input to send to the mapper
	 * 
	 * @param key
	 * @param val
	 */
	public void addSecondInput(final KIN2 key, final VIN2 val) {
		secondInputList.add(new Pair<KIN2, VIN2>(key, val));
	}

	/**
	 * Identical to addSecondInput() but returns self for fluent programming
	 * style
	 * 
	 * @param key
	 * @param val
	 * @return this
	 */
	public DualInputMapReduceDriver<KIN1, VIN1, KIN2, VIN2, KMAP, VMAP, KOUT, VOUT> withSecondInput(final KIN2 key,
			final VIN2 val) {
		addSecondInput(key, val);
		return this;
	}

	/**
	 * Identical to addInput() but returns self for fluent programming style
	 * 
	 * @param key
	 * @param val
	 * @return this
	 */
	public DualInputMapReduceDriver<KIN1, VIN1, KIN2, VIN2, KMAP, VMAP, KOUT, VOUT> withSecondInput(
			final Pair<KIN2, VIN2> pair) {
		secondInputList.add(pair);
		return this;
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<Pair<KOUT, VOUT>> run() throws IOException {
		if (inputList.isEmpty()) {
			throw new IllegalStateException("No input was provided");
		}
		if (firstMapper == null) {
			throw new IllegalStateException("First Mapper class was	provided");
		}

		if (secondMapper == null) {
			throw new IllegalStateException("Second Mapper class was provided");
		}

		if (reducer == null) {
			throw new IllegalStateException("No Reducer class was provided");
		}

		List<Pair<KMAP, VMAP>> mapOutputs = new ArrayList<Pair<KMAP, VMAP>>();

		// Run first map component
		for (final Pair<KIN1, VIN1> input : this.inputList) {
			LOG.debug("Mapping first input " + input.toString() + ")");

			mapOutputs.addAll(new MapDriver(firstMapper)

			.withConfiguration(configuration).withCounters(getCounters()).withInput(input).run());

		}

		// Run second map component
		for (final Pair<KIN2, VIN2> input : this.secondInputList) {
			LOG.debug("Mapping second input " + input.toString() + ")");

			mapOutputs.addAll(new MapDriver(secondMapper)

			.withConfiguration(configuration).withCounters(getCounters()).withInput(input).run());
		}

		// Configure grouping and sorting comparators
		if (keyGroupComparator instanceof Configured) {
			((Configured) keyGroupComparator).setConf(configuration);
		}
		if (keyValueOrderComparator instanceof Configured) {
			((Configured) keyValueOrderComparator).setConf(configuration);
		}

		if (this.getCombiner() != null) {
			// User has specified a combiner. Run this and replace the mapper
			// outputs
			// with the result of the combiner.
			LOG.debug("Starting combine phase with combiner: " + combiner);
			mapOutputs = new ReducePhaseRunner<KMAP, VMAP>().runReduce(shuffle(mapOutputs), combiner);
		}

		// Run the reduce phase.
		LOG.debug("Starting reduce phase with reducer: " + reducer);
		return new ReducePhaseRunner<KOUT, VOUT>().runReduce(shuffle(mapOutputs), reducer);
	}

	/**
	 * Have to copy class as its private.
	 */
	private class ReducePhaseRunner<OUTKEY, OUTVAL> {
		private List<Pair<OUTKEY, OUTVAL>> runReduce(final List<Pair<KMAP, List<VMAP>>> inputs,
				final Reducer<KMAP, VMAP, OUTKEY, OUTVAL> reducer) throws IOException {

			final List<Pair<OUTKEY, OUTVAL>> reduceOutputs = new ArrayList<Pair<OUTKEY, OUTVAL>>();

			for (final Pair<KMAP, List<VMAP>> input : inputs) {
				final KMAP inputKey = input.getFirst();
				final List<VMAP> inputValues = input.getSecond();
				final StringBuilder sb = new StringBuilder();
				formatValueList(inputValues, sb);
				LOG.debug("Reducing input (" + inputKey.toString() + ", " + sb.toString() + ")");

				@SuppressWarnings("unchecked")
				final ReduceDriver<KMAP, VMAP, OUTKEY, OUTVAL> reduceDriver = new ReduceDriver(reducer)
						.withCounters(getCounters())

						.withConfiguration(configuration).withInputKey(inputKey).withInputValues(inputValues);
				reduceOutputs.addAll(reduceDriver.run());
			}

			return reduceOutputs;
		}
	}
}
