package me.allenz.prime_calc;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * @author Allenz
 * @since 2014-07-28
 */
public class App {

	public static void main(final String[] args) throws NumberFormatException {
		if (args.length != 3) {
			throw new IllegalArgumentException(
					"parameters not found: target max poolSize");
		}
		final int target = Integer.parseInt(args[0]);
		final int max = Integer.parseInt(args[1]);
		final int poolSize = Integer.parseInt(args[2]);
		System.out.format("Java大法好\ntarget:%d,max:%d\n", target, max);
		final long start = System.currentTimeMillis();
		final int result = sieve3rd(target, max, poolSize);
		final long end = System.currentTimeMillis();
		System.out.format("result:%d,cost:%dms", result, end - start);
	}

	/**
	 * 埃氏筛
	 * 
	 * @param serial
	 *            素数序号
	 * @param max
	 *            筛选最大值
	 * @return 目标素数
	 */
	@SuppressWarnings("unused")
	private static int sieve1st(final int serial, final int max) {
		final boolean sieve[] = new boolean[max + 1];
		final int end = (int) Math.sqrt(max);
		for (int i = 2; i <= end; i++) {
			if (!sieve[i]) {
				for (int j = i + i; j < max; j += i) {
					sieve[j] = true;
				}
			}
		}
		sieve[0] = sieve[1] = true;
		int count = 0;
		for (int i = 0; i <= max; ++i) {
			if (!sieve[i]) {
				++count;
			}
			if (count == serial) {
				return i;
			}
		}
		return -1;
	}

	/**
	 * 奇数埃氏筛
	 * 
	 * @param serial
	 *            素数序号
	 * @param max
	 *            筛选最大值
	 * @return 目标素数
	 */
	@SuppressWarnings("unused")
	private static int sieve2nd(int serial, final int max) {
		final int size = max % 2 == 0 ? max >> 1 : (max >> 1) + 1;
		final boolean sieve[] = new boolean[size];
		final int endNumber = (int) Math.sqrt(max);
		final int endIndex = endNumber % 2 == 0 ? endNumber >> 1
				: (endNumber - 1) >> 1;
		for (int i = 1; i <= endIndex; ++i) {
			if (!sieve[i]) {
				final int number = (i << 1) + 1;
				final int number2 = number << 1;
				for (int j = number * number; j <= max; j += number2) {
					sieve[(j - 1) >> 1] = true;
				}
			}
		}
		int count = 0;
		serial--;
		for (int i = 1; i < size; ++i) {
			if (!sieve[i]) {
				++count;
			}
			if (count == serial) {
				return (i << 1) + 1;
			}
		}
		return -1;
	}

	/**
	 * 多线程分段奇数埃氏筛
	 * 
	 * @param serial
	 *            素数序号
	 * @param max
	 *            筛选最大值
	 * @param poolSize
	 *            线程数
	 * @return 目标素数
	 */
	private static int sieve3rd(final int serial, final int max,
			final int poolSize) {
		int endNumber = (int) Math.sqrt(max);
		endNumber = endNumber % 2 == 0 ? endNumber - 1 : endNumber;
		int maxSieveSize = (endNumber - 1) >> 1;
		final boolean sieve[] = new boolean[maxSieveSize + 1];
		final ArrayList<Integer> primes = new ArrayList<>(endNumber + 1);
		for (int i = 1; i <= maxSieveSize; ++i) {
			if (!sieve[i]) {
				final int number = (i << 1) + 1;
				primes.add(Integer.valueOf(number));// fucking auto boxing
				final int number2 = number << 1;
				for (int j = number * number; j <= endNumber; j += number2) {
					sieve[(j - 1) >> 1] = true;
				}
			}
		}
		int count = primes.size() + 1;
		final ExecutorService executorPool = Executors
				.newFixedThreadPool(poolSize);
		final List<Callable<SegmentSieve>> segmentCallables = new ArrayList<>(
				poolSize);
		final int firstSegmentsCount = poolSize - 1;
		final int restCount = max - endNumber - 1;
		maxSieveSize = restCount >> 1;
		final int segmentSieveSize = maxSieveSize / firstSegmentsCount;
		final int lastSegmentSieveSize = maxSieveSize % firstSegmentsCount;
		final int offset = endNumber - 1;
		for (int i = 0; i < firstSegmentsCount; i++) {
			final int segmentOffset = offset + segmentSieveSize * i * 2;
			segmentCallables.add(new SegmentCallable(segmentOffset,
					segmentSieveSize, primes));
		}
		if (lastSegmentSieveSize != 0) {
			final int segmentOffset = offset + segmentSieveSize
					* firstSegmentsCount * 2;
			segmentCallables.add(new SegmentCallable(segmentOffset,
					lastSegmentSieveSize, primes));
		}
		try {
			final List<Future<SegmentSieve>> results = executorPool.invokeAll(
					segmentCallables, 10000, TimeUnit.SECONDS);
			for (final Future<SegmentSieve> result : results) {
				final SegmentSieve segmentSieve = result.get();
				final int resultCount = segmentSieve.count;
				final int resultSieveSize = segmentSieve.sieve.length;
				if (count + resultCount > serial) {
					for (int i = 1; i < resultSieveSize; ++i) {
						if (!segmentSieve.sieve[i]) {
							++count;
						}
						if (count == serial) {
							return segmentSieve.offset + 1 + (i << 1);
						}
					}
				} else {
					count += resultCount;
					if (count == serial) {
						return segmentSieve.offset + 1
								+ ((resultSieveSize - 1) << 1);
					}
				}
			}
		} catch (final InterruptedException | ExecutionException e) {
			e.printStackTrace();
		} finally {
			executorPool.shutdown();
		}

		return -1;
	}

	private static class SegmentCallable implements Callable<SegmentSieve> {

		private int segmentOffset;
		private int segmentMaxIndex;
		private ArrayList<Integer> primes;

		public SegmentCallable(final int segmentOffset,
				final int segmentSteveSize, final ArrayList<Integer> primes) {
			this.segmentOffset = segmentOffset;
			this.segmentMaxIndex = segmentSteveSize;
			this.primes = new ArrayList<>(primes);
		}

		@Override
		public SegmentSieve call() throws Exception {
			final SegmentSieve segmentSieve = new SegmentSieve();
			segmentSieve.offset = segmentOffset;
			final boolean sieve[] = segmentSieve.sieve = new boolean[segmentMaxIndex + 1];
			final int segmentMaxNumber = segmentOffset + 1
					+ (segmentMaxIndex << 1);
			int count = 0;
			int size = primes.size();
			for (int i = 1; i <= segmentMaxIndex; ++i) {
				if (!sieve[i]) {
					final int number = segmentOffset + 1 + (i << 1);
					final int max = (int) Math.sqrt(number);
					int j = 0;
					while (j < size) {
						final int prime = primes.get(j);
						if (prime > max) {
							break;
						}
						if (number % prime == 0) {
							final int prime2 = prime << 1;
							for (int k = number; k <= segmentMaxNumber; k += prime2) {
								sieve[(k - segmentOffset - 1) >> 1] = true;
							}
							primes.remove(j);
							size = primes.size();
							break;
						}
						++j;
					}
					if (!sieve[i]) {
						++count;
					}
				}
			}

			segmentSieve.count = count;
			return segmentSieve;
		}
	}

	private static class SegmentSieve {
		public boolean sieve[];
		public int count;
		public int offset;
	}

}
