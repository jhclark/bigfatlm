package bigfat.diskcollections;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class QueueBuilderTest {

	private boolean reuseQueue;
	private int maxObjectsInMemory;
	private static QueueBuilder<Integer> queue;

	public QueueBuilderTest(boolean reuseQueue, int maxObjectsInMemory) {
		this.reuseQueue = reuseQueue;
		this.maxObjectsInMemory = maxObjectsInMemory;
		if (reuseQueue) {
			queue = new QueueBuilder<Integer>(maxObjectsInMemory);
		}
	}

	@Parameters
	public static Collection<?> regExValues() {
		return Arrays.asList(new Object[][] { { true, 1 }, { true, 2 }, { true, 3 }, { true, 4 },
				{ true, 10000 }, { false, 1 }, { false, 2 }, { false, 3 }, { false, 4 },
				{ false, 10000 }, });
	}

	@Before
	public void setup() {
		if (!reuseQueue) {
			queue = new QueueBuilder<Integer>(maxObjectsInMemory);
		}
	}

	@After
	public void cleanup() throws IOException {
		if (reuseQueue) {
			queue.clear();
		} else {
			queue.dispose();
		}
	}

	@AfterClass
	public static void cleanupClass() throws IOException {
		if (queue != null) {
			queue.dispose();
		}
	}

	@Test
	public void testInMemoryAlmostEmpty() throws IOException {
		test(0);
	}

	@Test
	public void testInMemoryAlmostFull() throws IOException {
		test(maxObjectsInMemory - 1);
	}

	@Test
	public void testInMemoryFull() throws IOException {
		test(maxObjectsInMemory);
	}

	@Test
	public void testOneOnDisk() throws IOException {
		test(maxObjectsInMemory + 1);
	}

	@Test
	public void testTwoOnDisk() throws IOException {
		test(maxObjectsInMemory + 2);
	}

	@Test
	public void testThreeOnDisk() throws IOException {
		test(maxObjectsInMemory + 3);
	}

	@Test
	public void testHalfOnDisk() throws IOException {
		test(maxObjectsInMemory * 2);
	}
	
	@Test
	public void testMultipleCycles() throws IOException {
		for(int i=0; i<10; i++) {
			// also test reading increasingly short lists back in
			test(maxObjectsInMemory * 2*(10-i));
			queue.clear();

			// garbage collection can trigger finalizers,
			// which in turn close file descriptors		
			System.gc();
		}
	}

	private void test(int numInList) throws IOException {
		for (int i = 0; i < numInList; i++) {
			queue.add(i);
		}

		Iterator<Integer> it = queue.reread().iterator();
		for (int i = 0; i < numInList; i++) {
			assertEquals(new Integer(i), it.next());
		}
	}
}
