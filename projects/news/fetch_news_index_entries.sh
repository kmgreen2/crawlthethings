#!/usr/bin/env bash

OUT_DIR=${1?Directory to write the index entries file (news-MM-YYYY.files)}
YEAR=${2?Year in YYYY format}
MONTH=${3?Month in MM format}
START_OFFSET=${4?Start offset in the index file}
LENGTH=${5?Number of index files to process}

aws s3 ls --recursive s3://commoncrawl/crawl-data/CC-NEWS/${YEAR}/${MONTH} | awk '{print $4;}'  \
| tail -n +${START_OFFSET} | head -n ${LENGTH} \
  > ${OUT_DIR}/news-${MONTH}-${YEAR}-${START_OFFSET}-${LENGTH}.files
