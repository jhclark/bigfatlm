package bigfat.diskcollections;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import com.google.common.collect.Iterators;

/**
 * A queue that automatically overflows to disk after a specified number of
 * objects are in memory. Usage pattern is iteratively write all data, then
 * iteratively read all data.
 * 
 * @author jon
 * @param <T>
 */
public class QueueBuilder<T extends Serializable> {

	public static final int BUFFER_SIZE = 10000;

	public static interface DiskQueueListener {
		public void notifyOfDiskHit();
	}

	public static class MemoryQueueReader<T> implements Iterable<T> {

		private final Iterable<T> queue;

		public MemoryQueueReader(Iterable<T> queue) {
			this.queue = queue;
		}

		@Override
		public Iterator<T> iterator() {
			return queue.iterator();
		}
	}

	public static class DiskQueueReader<T extends Serializable> implements Iterable<T> {

		private final File file;
		private final int numObjects;
		private final List<T> list;

		public DiskQueueReader(List<T> list, File file, int numObjects) {
			this.list = list;
			this.file = file;
			this.numObjects = numObjects;
		}

		@Override
		public Iterator<T> iterator() {
			try {
				// first read all the entries from memory,
				// then the ones that spilled onto disk
				return Iterators.concat(list.iterator(), new Iterator<T>() {

					private final ObjectInputStream input =
							new ObjectInputStream(new BufferedInputStream(new FileInputStream(
									file), BUFFER_SIZE));
					private int i = 0;

					@Override
					public boolean hasNext() {
						return i < numObjects;
					}

					@SuppressWarnings("unchecked")
					@Override
					public T next() {
						if (!hasNext()) {
							throw new NoSuchElementException();
						}
						try {
							i++;
							return (T) input.readObject();
						} catch (IOException e) {
							throw new RuntimeException(e);
						} catch (ClassNotFoundException e) {
							throw new RuntimeException(e);
						}
					}

					@Override
					public void remove() {
						throw new UnsupportedOperationException();
					}

				});
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	private final List<T> list;
	private final int maxObjectsInMemory;
	private int diskObjectCount = 0;
	RandomAccessFile raFile = null;
	private ObjectOutputStream output = null;
	private boolean rewound = false;
	private File tempFile;
	private DiskQueueListener listener;
	private boolean disposed;

	public QueueBuilder(int maxObjectsInMemory) {
		this.maxObjectsInMemory = maxObjectsInMemory;
		list = new ArrayList<T>(maxObjectsInMemory);
	}

	public void setListener(DiskQueueListener listener) {
		this.listener = listener;
	}

	public void add(T obj) throws IOException {
		
		if (rewound) {
			throw new RuntimeException("List has been rewound. No more objects can be added.");
		}
		if (disposed) {
			throw new IllegalStateException("Queue is disposed");
		}
		if (list.size() < maxObjectsInMemory) {
			list.add(obj);

		} else {

			if (diskObjectCount == 0) {
				if (listener != null)
					listener.notifyOfDiskHit();
				if (raFile == null) {
					tempFile = File.createTempFile(QueueBuilder.class.getName(), ".tmp");
					// don't keep opening and closing file objects
					raFile = new RandomAccessFile(tempFile, "rw");
					output =
						new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(
								raFile.getFD()), BUFFER_SIZE));
				}
			}

			diskObjectCount++;
			output.writeObject(obj);
		}
	}

	public Iterable<T> reread() throws IOException {
		if (disposed) {
			throw new IllegalStateException("Queue is disposed");
		}
		// keeping the rewound state allows us to avoid the issue
		// of contant flushing and changing object counts
		rewound = true;
		if (diskObjectCount == 0) {
			return new MemoryQueueReader<T>(list);
		} else {
			output.flush();
			if(raFile != null) {
				raFile.close();
				raFile = null;
			}
			
			return new DiskQueueReader<T>(list, tempFile, diskObjectCount);
		}
	}

	public int getDiskEntryCount() {
	    return diskObjectCount;
	}

	public void dispose() throws IOException {
		if(raFile != null) {
			raFile.close();
			raFile = null;
		}
		if (tempFile != null) {
			tempFile.delete();
			tempFile = null;
		}
		disposed = true;
	}

	public void clear() throws IOException {
		rewound = false;
		diskObjectCount = 0;
		list.clear();
		if (raFile != null) {
			raFile.seek(0);
			raFile.setLength(0);
		}
	}

	public int size() {
		return list.size() + diskObjectCount;
	}
}
