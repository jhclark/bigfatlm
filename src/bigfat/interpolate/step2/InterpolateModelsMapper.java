package bigfat.interpolate.step2;

import jannopts.Option;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import bigfat.BigFatLM;
import bigfat.datastructs.FastIntArrayList;
import bigfat.datastructs.Ngram;
import bigfat.hadoop.HadoopUtils;
import bigfat.step5.DuplicatableNgram;
import bigfat.step5.InterpolateOrdersPartitioner;

/**
 * 
 */
public class InterpolateModelsMapper extends
		Mapper<Ngram, InterpolateModelsVector, DuplicatableNgram, InterpolateModelsVector> {

	private final FastIntArrayList ids = new FastIntArrayList(5);
	private final DuplicatableNgram reversedNgram = new DuplicatableNgram();
	private final InterpolateOrdersPartitioner partitioner = new InterpolateOrdersPartitioner();
	
	private Counter[] ngramsRead;
	private Counter[] ngramsWritten;
	
	@Option(shortName = "N", longName = "order", usage = "order of the language model")
	private int maxOrder;
	
	@Option(shortName = "r", longName = "numReducers", usage = "number of reducers to be used")
	private int numReducers;

	@Override
	public void setup(Context context) {
		HadoopUtils.configure(context.getConfiguration(), this);
		
		this.ngramsRead = new Counter[maxOrder + 1];
		this.ngramsWritten = new Counter[maxOrder + 1];
		for (int i = 0; i <= maxOrder; i++) {
			ngramsRead[i] =
					context.getCounter(BigFatLM.PROGRAM_NAME, i + "-gram uninterpolated probabilities read");
			ngramsWritten[i] =
					context.getCounter(BigFatLM.PROGRAM_NAME, i + "-gram uninterpolated probabilities written");
		}
	}

	@Override
	public void map(Ngram ngram, InterpolateModelsVector value,
			Context hadoopContext) {

		try {
			ngram.getAsIds(ids);
			int order = ids.size();

			ngramsRead[order].increment(1);

			final boolean REVERSE_NGRAM = true;
			if (order > 1) {
				int part = partitioner.getNonUnigramPartition(ids, numReducers);
				reversedNgram.setFromIds(part, ids, REVERSE_NGRAM);
				hadoopContext.write(reversedNgram, value);
				ngramsWritten[order].increment(1);
			} else {
				// broadcast unigrams and zero-tons (unk) to all reducers
				for (int iReducer = 0; iReducer < numReducers; iReducer++) {
					reversedNgram.setFromIds(iReducer, ids, REVERSE_NGRAM);
					hadoopContext.write(reversedNgram, value);
					ngramsWritten[order].increment(1);
				}
			}

		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
