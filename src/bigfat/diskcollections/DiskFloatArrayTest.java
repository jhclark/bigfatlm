package bigfat.diskcollections;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

public class DiskFloatArrayTest {
	
	public static final float DELTA = 0.0f;

	@Test
	public void test() throws IOException {
		DiskFloatArray arr = new DiskFloatArray(File.createTempFile("diskfloat", "test"));
		Assert.assertEquals(0.0, arr.get(0), DELTA);
		Assert.assertEquals(0.0, arr.get(100), DELTA);
		
		arr.set(0, 1.0f);		
		Assert.assertEquals(1.0, arr.get(0), DELTA);
		
		arr.set(100, 1.5f);		
		Assert.assertEquals(1.5, arr.get(100), DELTA);
		Assert.assertEquals(0.0, arr.get(50), DELTA);
	}
}
