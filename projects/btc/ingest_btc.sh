#!/usr/bin/env bash

ROOTDIR=`dirname $0`/../..

START_BLOCK=${1?Start block}
END_BLOCK=${2?End block}
BATCH_SIZE=${3?Batch size}
THREADS=${4?Number of threads}
# Format 'region.bucket'
REGION_BUCKET_NAME=${5}

OUTPUT_SPEC=file:///tmp/btc

if (( ${END_BLOCK} < ${START_BLOCK} )); then
  echo "Start block must be <= end block"
  exit 1
fi

if [[ -n ${REGION_BUCKET_NAME} ]]; then
    OUTPUT_SPEC=s3://${REGION_BUCKET_NAME}/btc
fi

let CURR_START_BLOCK=${START_BLOCK}
let CURR_END_BLOCK=${CURR_START_BLOCK}+${BATCH_SIZE}-1
if (( ${CURR_END_BLOCK} > ${END_BLOCK} )); then
    CURR_END_BLOCK=${END_BLOCK}
fi

while (( ${CURR_END_BLOCK} <= ${END_BLOCK} )); do
  pipenv run python3 ./src/main.py -i "https://blockchain.info/block-height,${CURR_START_BLOCK},${CURR_END_BLOCK}" \
    -o "${OUTPUT_SPEC}-${CURR_START_BLOCK}-${CURR_END_BLOCK}" -p copy  -I btc -t "${THREADS}"
  if (( ${CURR_END_BLOCK} == ${END_BLOCK} )); then
    break
  fi
  let CURR_START_BLOCK=${CURR_END_BLOCK}+1
  let CURR_END_BLOCK=${CURR_END_BLOCK}+${BATCH_SIZE}
  if (( ${CURR_END_BLOCK} > ${END_BLOCK} )); then
    CURR_END_BLOCK=${END_BLOCK}
  fi
done


