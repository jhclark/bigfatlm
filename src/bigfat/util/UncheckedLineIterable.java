package bigfat.util;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.google.common.collect.PeekingIterator;

public class UncheckedLineIterable implements Iterable<String>, Closeable {

	private final BufferedReader in;
	private boolean usedOnce = false;

	public UncheckedLineIterable(InputStream stream, Charset charset) throws FileNotFoundException {
		this.in = new BufferedReader(new InputStreamReader(stream, charset));
	}

	public static class UncheckedLineIterator implements PeekingIterator<String>, Closeable {
		private String line = null;
		private final BufferedReader in;

		public UncheckedLineIterator(BufferedReader in) {
			this.in = in;
		}

		public UncheckedLineIterator(InputStream stream, Charset charset) {
			this.in = new BufferedReader(new InputStreamReader(stream, charset));
		}

		@Override
		public boolean hasNext() {
			try {
				if (line == null) {
					line = in.readLine();
				}
				return (line != null);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public String next() {
			String tmp = peek();
			line = null;
			return tmp;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

		@Override
		public String peek() {
			// note nasty use of hasNext() side effect
			if (hasNext()) {
				return line;
			} else {
				throw new NoSuchElementException();
			}
		}

		@Override
		public void close() throws IOException {
			this.in.close();
		}
	}

	@Override
	public Iterator<String> iterator() {
		if (usedOnce) {
			throw new IllegalStateException("UncheckedLineIterable can only be iterated over once.");
		}
		usedOnce = true;
		return new UncheckedLineIterator(in);
	}

	@Override
	public void close() throws IOException {
		this.in.close();
	}
}
