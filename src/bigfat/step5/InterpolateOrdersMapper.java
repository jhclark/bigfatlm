package bigfat.step5;

import jannopts.Option;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import bigfat.BigFatLM;
import bigfat.datastructs.FastIntArrayList;
import bigfat.datastructs.Ngram;
import bigfat.hadoop.HadoopUtils;
import bigfat.step4.UninterpolatedInfo;

// TODO: Special class for InterpolatedIntermediateValue?
public class InterpolateOrdersMapper extends
		Mapper<Ngram, UninterpolatedInfo, DuplicatableNgram, InterpolateOrdersIntermediateInfo> {

	private final FastIntArrayList ids = new FastIntArrayList(5);
	private final DuplicatableNgram reversedNgram = new DuplicatableNgram();
	private final InterpolateOrdersPartitioner partitioner = new InterpolateOrdersPartitioner();
	private final InterpolateOrdersIntermediateInfo intInfo = new InterpolateOrdersIntermediateInfo();
	
	@Option(shortName = "N", longName = "order", usage = "order of the language model")
	private int maxOrder;
	
	@Option(shortName = "r", longName = "numReducers", usage = "number of reducers to be used")
	private int numReducers;

	private Counter[] ngramsRead;
	private Counter[] ngramsWritten;

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
	public void map(Ngram history, UninterpolatedInfo value, Context hadoopContext) {
		try {
			
			history.getAsIds(ids);
			ids.add(value.getWord());
						
			int order = ids.size();
			
			ngramsRead[order].increment(1);
		
			intInfo.setValues(value.getUninterpolatedProb(), value.getInterpolationWeight());

			final boolean REVERSE_NGRAM = true;
			if (order > 1) {
				int part = partitioner.getNonUnigramPartition(ids, numReducers);
				reversedNgram.setFromIds(part, ids, REVERSE_NGRAM);
				hadoopContext.write(reversedNgram, intInfo);
				ngramsWritten[order].increment(1);
			} else {
				// broadcast unigrams and zero-tons to all reducers
				for (int iReducer = 0; iReducer < numReducers; iReducer++) {
					reversedNgram.setFromIds(iReducer, ids, REVERSE_NGRAM);
					hadoopContext.write(reversedNgram, intInfo);
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
