#!/usr/bin/env bash
set -e
set -o pipefail
set -x

regDir=$(readlink -f $(dirname $0))
rootDir=$regDir/..
dataDir=$regDir/data
tmpDir=$regDir/tmp

mkdir -p $tmpDir
cd $tmpDir
rm -rf reg-test-3g reg-test-3g.arpa
$rootDir/build-lm.sh 3 $dataDir/news-parl-1M reg-test-3g reg-test-3g.arpa
$regDir/lm-diff.py $dataDir/news-parl-1M-3g-noprune-imkn-unk.arpa reg-test-3g.arpa
