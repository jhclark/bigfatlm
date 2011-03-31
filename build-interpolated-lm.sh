#!/usr/bin/env bash
set -e
set -o pipefail

scriptDir=$(dirname $0)

function usage {
    echo >&2 "Usage: $0 order tuneCorpus trainCorporaFile outDirHdfs outDirLocal finalLocalLmFile"
    echo >&2 ""
    echo >&2 "trainCorpora file should contain names of LMs to be interpolated, one per line 'nameTABpath' where the first line must be the corpus which is the union or a superset of all following corpora"
    exit 1
}

if [[ $# != 6 ]]; then
    usage
fi

set -x

# 0) Parse n paths to input corpora pieces
# TODO: Better opt parsing for picking up at some stage
order=$1
tuneCorpus=$2
trainCorporaFile=$3 # file containing names of LMs to be interpolated, one per line nameTABpath
outDirHdfs=$4
outDirLocal=$5
finalLm=$6

# Make sure we have the filter program before going through
# the trouble of building the models
filterBin=$scriptDir/filter
if [ ! -e $filterBin ]; then
    echo >&2 "Could not find KenLM filter binary: $filterBin"
    exit 1
fi

supersetCorpusHdfs=$(awk 'NR==1{print $2}' $trainCorporaFile)
fullBinLmsFile=$outDirLocal/lmBinPaths.txt
fullTextLmsFile=$outDirLocal/lmTextPaths.txt
tuneFilteredLmsFile=$outDirLocal/tuneLms.txt
weightsFile=$outDirLocal/weights.txt
vocabHdfs=$outDirHdfs/vocabIds.txt

# Validate format of trainCorporaFile
cat $trainCorporaFile | while read line; do
    name=$(echo "$line" | awk '{print $1}')
    path=$(echo "$line" | awk '{print $2}')
    if [ -z "$name" ]; then
	echo >&2 "Invalid trainCorporaFile, no LM Name: " $line
	exit 1
    fi
    if [ -z "$path" ]; then
	echo >&2 "Invalid trainCorporaFile, no LM path: " $line
	exit 1
    fi
done

# 0) Build a superset vocabulary file so that the final model's
#    text format can be recovered properly
#skipVocab=1
if [ -z $skipVocab ]; then
    $scriptDir/bigfat vocab \
	--corpusIn $supersetCorpusHdfs \
	--vocabIdsOut $vocabHdfs
fi

# 1) Build n LMs
#skipBuild=1
if [ -z $skipBuild ]; then
    rm -f $fullLmsFile
    cat $trainCorporaFile | while read line; do
	lmName=$(echo $line | awk '{print $1}')
	hdfsInPath=$(echo $line | awk '{print $2}')
	hdfsOutPath=$outDirHdfs/$lmName
	build-lm.sh $order $hdfsInPath $hdfsOutPath NULL \
	    --skipArpaFilter \
	    --skipArpaMerge \
	    --skipVocab \
	    --vocabHdfs $vocabHdfs

	hdfsBinModelOut=$outDirHdfs/$lmName/renormModelDir
	hdfsTextModelOut=$outDirHdfs/$lmName/dArpaModelDir
	echo "$lmName $hdfsBinModelOut" >> $fullBinLmsFile
	echo "$lmName $hdfsTextModelOut" >> $fullTextLmsFile
    done
fi

# 2) Filter LMs to tune corpus
#skipFilter=1
if [ -z $skipFilter ]; then
    rm -rf $tuneFilteredLmsFile
    cat $fullTextLmsFile | while read line; do
	lmName=$(echo $line | awk '{print $1}')
	hdfsInDarpaPath=$(echo $line | awk '{print $2}')
	localFilteredArpa=$outDirLocal/${lmName}.arpa
	$scriptDir/bigfat filterForPP $hdfsInDarpaPath $tuneCorpus $localFilteredArpa

	echo "$lmName $localFilteredArpa" >> $tuneFilteredLmsFile
    done
fi

# 3) Feed filtered LMs and tune corpus to optimizer
# Make semicolon-delimited list of filtered LM paths
#skipOptimize=1
if [ -z $skipOptimize ]; then
    inputArpas=$(awk '{printf "%s;",$2}' $tuneFilteredLmsFile)
    $scriptDir/bigfat findModelInterpWeights \
	--corpusIn $tuneCorpus \
	--modelsIn $inputArpas \
	| tee $weightsFile
fi

# Generate modelFile/weights string with MasterLM first
hdfsModelsIn=$(awk '{printf "%s;",$2}' $fullBinLmsFile)
weights=$(awk '{printf "%s;",$2}' $weightsFile)

# 4) Expand the models so that each n-gram has a vector
#    with the probability and backoff weight (FOR ITS OWN CONTEXT)
#    for each n-gram locally
#skipVectorize=1
if [ -z $skipVectorize ]; then
    # TODO: Fix the need to duplicate weights on the command line...
    $scriptDir/bigfat makeModelVectors \
	--modelsIn $hdfsModelsIn \
	--modelsIn2 $hdfsModelsIn \
	--weights $weights \
	--maxOrder $order \
	--modelOut $outDirHdfs/modelInterp/vectorModelDir
fi

# 5) Weight each LM in the vector and do backoff inference
#    as necessary
#skipInterp=1
if [ -z $skipInterp ]; then
    $scriptDir/bigfat interpModels \
	--modelsIn $outDirHdfs/modelInterp/vectorModelDir \
	--weights $weights \
	--order $order \
	--maxOrder $order \
	--modelOut $outDirHdfs/modelInterp/unnormInterpDir
fi

# 6) Renormalize the backoffs for the interpolated probabilities
#skipRenorm=1
if [ -z $skipRenorm ]; then 
    $scriptDir/bigfat renorm \
	--modelIn $outDirHdfs/modelInterp/unnormInterpDir \
	--modelOut $outDirHdfs/modelInterp/renormModelDir \
	--maxOrder $order \
	--order $order \
	--debug false
fi

# 7) Text-ify the model (write dARPA)
#skipTextify=1
if [ -z $skipTextify ]; then
    $scriptDir/bigfat darpa \
	--modelIn $outDirHdfs/modelInterp/renormModelDir \
	--dArpaOut $outDirHdfs/modelInterp/dArpaDir \
	--order $order \
	--vocabFile $vocabHdfs
fi

# 8) Merge dARPA to a single gzipped on-disk ARPA
#skipMerge=1
if [ -z $skipMerge ]; then
    $scriptDir/bigfat mergearpa \
	--dArpaIn $outDirHdfs/modelInterp/dArpaDir \
	--arpaFileOut $finalLm
fi
