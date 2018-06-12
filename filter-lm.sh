#!/usr/bin/env bash
set -e
set -o pipefail

scriptDir=$(dirname $0)

function usage {
    echo "Usage: $0 dArpaModelHdfsIn filterVocabHdfsIn filteredModelHdfsOut [phrase]"
    exit 1
}

if [[ $# != 3 && $# != 4 ]]; then
    usage
fi

if [[ $# == 4 && "$4" != "phrase" ]]; then
    usage
fi

set -x

dArpaModelHdfs=$1 #input
filterVocabHdfs=$2 # file to filter to
filterModelHdfs=$3 #output
usePhrase=$4 # either "phrase" or empty

# TODO: Grab task count from external config?
numTasks=400

localTmpDir=tmp/BigFatLM-$RANDOM
mkdir -p $localTmpDir
trap "rm -rf $localTmpDir" EXIT

filterBin=$scriptDir/filter

if [ ! -e $filterBin ]; then
    echo >&2 "Could not find KenLM filter binary: $filterBin"
    exit 1
fi

if which hadoop; then
    haveHadoop=1
else
    haveHadoop=""
fi

if [ $haveHadoop ]; then
    hadoop dfs -rmr $filterModelHdfs
    hadoop dfs -rmr $filterBin
    hadoop dfs -put $filterBin $filterBin
    (cat <<EOF
#!/usr/bin/env bash
set -e
set -o pipefail
set -x

find . >&2
# Hadoop will give us input on stdin
# The vocab file is provided via a distributed cache
./filter union $usePhrase raw vocab:$filterVocabHdfs /dev/stdout | \
   tee <(cut -f1 | awk '{if(NF>0){print "reporter:counter:BigFatLM,"NF"-gram dARPA entries written,1"}; if(NF>5){print "WARNING: "\$0}}' >/dev/stderr || kill -9 $PPID)
EOF
)>$localTmpDir/filter-mapper.sh


    frun="hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming.jar \
    -Dmapred.job.queue.name=default \
    -Dmapred.map.tasks=$numTasks \
    -Dmapred.reduce.tasks=$numTasks \
    -Dmapred.map.tasks.speculative.execution=True \
    -Dmapred.reduce.tasks.speculative.execution=True \
    -Dmapred.output.compress=true \
    -Dmapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec"

    $frun -Dmapred.job.name="BigFatLM -- Filter dARPA to test set: $testOut" \
	-files $filterBin,$filterVocabHdfs \
	-mapper $localTmpDir/filter-mapper.sh \
	-file $localTmpDir/filter-mapper.sh \
	-numReduceTasks 0 \
	-input $dArpaModelHdfs \
	-output $filterModelHdfs
else 
    # We're running locally
    rm -rf $filterModelHdfs
    if (( $(ls $dArpaModelHdfs/part* | egrep -c '\.bz2$') > 0 )); then
	catCmd="bzcat $dArpaModelHdfs/part*"
    elif (( $(ls $dArpaModelHdfs/part* | egrep -c '\.gz$') > 0 )); then
	catCmd="zcat $dArpaModelHdfs/part*"
    else
	catCmd="cat $dArpaModelHdfs/part*"
    fi
    $filterBin union $usePhrase raw vocab:$filterVocabHdfs $filterModelHdfs < <($catCmd)
fi