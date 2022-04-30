#!/usr/bin/env bash

ROOTDIR=`dirname $0`/../..

YEAR=${1?Year in YYYY format}
MONTH=${2?Month in MM format}
START_OFFSET=${3?Start offset in the index file}
LENGTH=${4?Number of index files to process}
# Format 'region.bucket'
REGION_BUCKET_NAME=${5}

${ROOTDIR}/projects/news/fetch_news_index_entries.sh ${ROOTDIR} ${YEAR} ${MONTH} ${START_OFFSET} ${LENGTH} || exit 1

if [[ -n ${REGION_BUCKET_NAME} ]]; then
    pipenv run python3 ${ROOTDIR}/src/main.py -i ${ROOTDIR}/news-${MONTH}-${YEAR}-${START_OFFSET}-${LENGTH}.files \
    -o s3://${REGION_BUCKET_NAME}/${YEAR}-${MONTH}-${START_OFFSET}-${LENGTH}.json -p news  -I warc-index -t 32
else
    pipenv run python3 ${ROOTDIR}/src/main.py -i ${ROOTDIR}/news-${MONTH}-${YEAR}-${START_OFFSET}-${LENGTH}.files \
    -o file:///tmp/crawlthenews-${YEAR}-${MONTH}-${START_OFFSET}-${LENGTH}.json -p news  -I warc-index -t 32
fi

