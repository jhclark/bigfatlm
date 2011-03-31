#!/usr/bin/env python
import sys
import math
from collections import defaultdict

(myLM, goldLM) = sys.argv[1:]

# TODO: Check header
# TODO: Check i-gram tags
# TODO: Test begin
# TODO: Test end

# First, read gold LM

def parseNgram(line):

    columns = line.strip().split('\t')
    (prob, ngram) = columns[:2]

    prob = float(prob)

    if len(columns) == 3:
        backoff = float(columns[2])
    elif len(columns) == 2:
        backoff = 0.0
    else:
        print >>sys.stderr, 'Badly formatted line in gold LM:', line
        sys.exit(1)

    return (ngram, prob, backoff)

print >>sys.stderr, 'Loading gold LM...'

gold = dict()
goldHeader = defaultdict(lambda: 0)
for line in open(goldLM):
    if not line.startswith('-'):
        continue

    (ngram, prob, backoff) = parseNgram(line)

    gold[ngram] = (prob, backoff)

    order = ngram.count(' ')+1
    goldHeader[order] += 1

print >>sys.stderr,'Done reading gold LM'
for order, count in goldHeader.iteritems():
    print >>sys.stderr, 'Read %d %d-grams'%(count, order)

# TODO: Make sure we recall all n-grams

# Epsilon is in log space and will be much smaller in prob space
EPSILON = 0.01
MAX_ERRORS = 100000

worstNgramProb = defaultdict(lambda: ('', 0.0, 0.0, 0.0))
worstNgramBackoff = defaultdict(lambda: ('', 0.0, 0.0, 0.0))

correct = defaultdict(lambda: 0)
backoffErrors = defaultdict(lambda: 0)
probErrors = defaultdict(lambda: 0)
tooManyErrors = defaultdict(lambda: 0)
notEnoughErrors = defaultdict(lambda: 0)
for line in open(myLM):
    if not line.startswith('-'):
        continue

    (ngram, prob, backoff) = parseNgram(line)
    order = ngram.count(' ')+1

    ok = True
    try:
        (goldProb, goldBackoff) = gold.pop(ngram)
    except:
        tooManyErrors[order] += 1
        ok = False
        if tooManyErrors[order] < MAX_ERRORS:
            print 'Additional n-gram not in gold LM: "%s"'%ngram
        continue
    
    logProbDiff = abs(prob - goldProb)
    probDiff = abs(math.pow(10.0, prob) - math.pow(10.0, goldProb))
    if logProbDiff > EPSILON:
        probErrors[order] += 1
        ok = False
        if probErrors[order] < MAX_ERRORS:
            print 'Unequal prob "%s" me=%f gold=%f logProbDiff=%f probDiff=%f'%(ngram, prob, goldProb, logProbDiff, probDiff)

    if probDiff > worstNgramProb[order][3]:
        worstNgramProb[order] = (ngram, prob, goldProb, logProbDiff, probDiff)

    backoffDiff = abs(backoff - goldBackoff)
    if backoffDiff > EPSILON:
        backoffErrors[order] += 1
        ok = False
        if backoffErrors[order] < MAX_ERRORS:
            print 'Unequal backoff "%s" me=%f gold=%f diff=%f'%(ngram, backoff, goldBackoff, backoffDiff)

    if backoffDiff > worstNgramBackoff[order][3]:
        worstNgramBackoff[order] = (ngram, backoff, goldBackoff, backoffDiff)

    if ok:
        correct[order] += 1

# Check for missing n-grams
for ngram in gold.iterkeys():
    order = ngram.count(' ')+1
    notEnoughErrors[order] +=1
    if notEnoughErrors[order] < MAX_ERRORS:
        print 'Missing n-gram: "%s"'%ngram

for order in goldHeader.iterkeys():
    print >>sys.stderr, '---'
    print >>sys.stderr, '%d-grams correct:'%order, correct[order]
    print >>sys.stderr, '%d-gram Prob errors:'%order, probErrors[order]
    print >>sys.stderr, '%d-gram Backoff errors:'%order, backoffErrors[order]
    print >>sys.stderr, '%d-gram Extra n-grams:'%order, tooManyErrors[order]
    print >>sys.stderr, '%d-gram Missing n-grams:'%order, notEnoughErrors[order]
    print >>sys.stderr, 'Worst %d-gram prob diff:'%order, worstNgramProb[order]
    print >>sys.stderr, 'Worst %d-gram backoff diff:'%order, worstNgramBackoff[order]

print >>sys.stderr, '---'
if (sum(probErrors.itervalues()) > 0
    or sum(backoffErrors.itervalues()) > 0
    or sum(tooManyErrors.itervalues()) > 0
    or sum(notEnoughErrors.itervalues()) > 0):
    print >>sys.stderr, 'FAIL: Language models differ'
    sys.exit(1)
else:
    print >>sys.stderr, 'PASS: Language models are equal (subject to floating point error)'
    sys.exit(0)
