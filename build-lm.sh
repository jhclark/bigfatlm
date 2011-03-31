#!/usr/bin/env bash
set -e
set -o pipefail

scriptDir=$(dirname $0)

function usage {
    echo >&2 "Usage: $0 order corpusHdfs lmName finalArpaFile [filterVocabHdfs] [skipOpts...]"
    echo >&2 ""
    echo >&2 "TIP: To obtain a gzipped final LM file use a process substitution >(gzip > lm.gz)"
    echo >&2 "TIP: To obtain a gzipped final LM file on a remote machine, use a process substitution >(bzip2 | ssh my.machine.com 'bunzip2 | gzip > /path/lm.gz' )"
    echo >&2 "TIP: To obtain a KenLM mmappable final LM file use a process substitution >($KENLM/build-binary /dev/stdin lm.mmap)"
    exit 1
}

if (( $# < 4 )); then
    usage
fi

set -x

order=$1
corpusHdfs=$2
name=$3 # HDFS directory where temporary files will be placed
finalArpaGz=$4
shift 4

if [[ "$1" != "" && "$1" != --* ]]; then
    noFilter=0
    filterVocabHdfs=$1
    shift
else
    noFilter=1
fi

function die {
    echo >&2 "$@"
    exit 1
}

[[ "$order" != --* ]] || die "invalid order: $order"
[[ "$corpusHdfs" != --* ]] || die "invalid corpusHdfs: $corpusHdfs"
[[ "$name" != --* ]] || die "invalid name: $name"
[[ "$finalArpaGz" != --* ]] || die "invalid finalArpaGz: $finalArpaGz"

localDir=$name

function parseArgs {
    while [[ $# > 0 ]]; do
	case "$1" in
	    --skipVocab) skipVocab=1 ;;
	    --skipExtract) skipExtract=1 ;;
	    --skipDiscounts) skipDiscounts=1 ;;
	    --skipUninterp) skipUninterp=1 ;;
	    --skipInterpOrders) skipInterp=1 ;;
	    --skipRenorm) skipRenorm=1 ;;
	    --skipArpaFormat) skipArpaFormat=1 ;;
	    --skipArpaFilter) skipArpaFilter=1 ;;
	    --noFilter) noFilter=1 ;;
	    --skipArpaMerge) skipArpaMerge=1 ;;
	    
	    --vocabHdfs) shift; vocabHdfs=$1 ;;

	    *) echo "Unrecognized flag: $1"; usage
	esac
	shift
    done
}

# A statically linked (possibly non-threaded) version of Kenneth's filter
filterVocabHdfs=vocabMT06.txt

testOut=$name
vocabHdfs=$testOut/vocabIds.txt
countsHdfs=$testOut/countsDir
countCountsTmp=$testOut/countCounts.txt
discountsHdfs=$testOut/discounts.txt
uninterpStatsHdfs=$testOut/uninterpStatsDir
interpModelHdfs=$testOut/interpModelDir
renormModelHdfs=$testOut/renormModelDir
dArpaModelHdfs=$testOut/dArpaModelDir
filterModelHdfs=$testOut/filterModelDir
arpaFileGz=$finalArpaGz

parseArgs $@

# Do we have a proper hadoop installation on this machine?
if which hadoop; then
    haveHadoop=1
else
    haveHadoop=""
fi

function hrm {
    file=$1
    if [ $haveHadoop ]; then (set +e; hadoop dfs -rmr $file); fi
    rm -rf $file
}

function hmkdir {
    dir=$1
    mkdir -p $dir
    if [ $haveHadoop ]; then hadoop dfs -mkdir $dir; fi
}

mkdir -p $localDir

if [ -z $skipVocab ]; then
    hmkdir $testOut
    hrm $vocabHdfs
    rm -rf $vocab
    $scriptDir/bigfat vocab \
	--corpusIn $corpusHdfs \
	--vocabIdsOut $vocabHdfs \
	2>&1 | tee $testOut/vocab.log
fi

if [ -z $skipExtract ]; then
    rm -rf $countsHdfs
    hrm $countsHdfs
    $scriptDir/bigfat extract \
	--vocabIds $vocabHdfs \
	--corpusIn $corpusHdfs \
	--countsOut $countsHdfs \
	--order $order \
	--maxOrder $order \
	--reverseKeys true \
	2>&1 | tee $testOut/extract.log
fi

### # This step is for debugging only
###rm -f $testOut/counts.txt
###java -cp bigfat.jar util.DebugSequenceFileReader adjusted-counts $vocabHdfs $countsHdfs $testOut/counts.txt

if [ -z $skipDiscounts ]; then
    hrm $countCountsTmp
    hrm $discountsHdfs
    $scriptDir/bigfat discounts \
	--countsIn $countsHdfs \
	--order $order \
	--maxOrder $order \
	--discountsOut $discountsHdfs \
	--countOfCountsTmpOut $countCountsTmp \
	2>&1 | tee $testOut/discounts.log
fi

if [ -z $skipUninterp ]; then
    hrm $uninterpStatsHdfs
    $scriptDir/bigfat uninterp \
	--adjustedCountsIn $countsHdfs \
	--statsOut $uninterpStatsHdfs \
	--order $order \
	--maxOrder $order \
	--discountFile $discountsHdfs \
	--minCountsPre 1,1,1,1,1 \
	--minCountsPost 1,1,1,1,1 \
	--debug false \
	2>&1 | tee $testOut/uninterp.log
fi

if [ -z $skipInterp ]; then
    hrm $interpModelHdfs
    # bigfat script will add numReducers for us
    $scriptDir/bigfat interpOrders \
	--uninterpolatedIn $uninterpStatsHdfs \
	--interpolatedOut $interpModelHdfs \
	--maxOrder $order \
	--order $order \
	--debug false \
	2>&1 | tee $testOut/interp.log
fi

if [ -z $skipRenorm ]; then
    hrm $renormModelHdfs
    $scriptDir/bigfat renorm \
	--modelIn $interpModelHdfs \
	--modelOut $renormModelHdfs \
	--maxOrder $order \
	--order $order \
	--debug false \
	2>&1 | tee $testOut/renorm.log
fi

if [ -z $skipArpaFormat ]; then
    hrm $dArpaModelHdfs
    $scriptDir/bigfat darpa \
	--modelIn $renormModelHdfs \
	--dArpaOut $dArpaModelHdfs \
	--order $order \
	--vocabFile $vocabHdfs \
	2>&1 | tee $testOut/darpa.log
fi

countsByOrder=$testOut/countsByOrder.txt
if [[ $noFilter == 1 ]]; then
    filterModelHdfs=$dArpaModelHdfs

    for i in $(seq 1 $order); do echo $i $(fgrep "$i-gram dARPA entries written" $testOut/darpa.log | cut -d= -f2); done > $countsByOrder

elif [ -z $skipArpaFilter ]; then
    $scriptDir/bigfat filter $dArpaModelHdfs $filterVocabHdfs $filterModelHdfs \
	2>&1 | tee $testOut/filter.log
fi

if [ -z $skipArpaMerge ]; then
    # Don't try to remove named pipes or process subsitution commands
    if [ ! -p $arpaFileGz ]; then
	rm -f $arpaFileGz
    fi
    $scriptDir/bigfat mergearpa \
	--dArpaIn $filterModelHdfs \
	--countsByOrder $countsByOrder \
	--arpaFileOut $arpaFileGz \
	2>&1 | tee $testOut/merge.log
fi