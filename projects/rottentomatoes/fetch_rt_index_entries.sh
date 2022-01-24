#!/usr/bin/env bash
CDX_INDEX_CLIENT=${1?Location of cdx-index-client.py}
OUTDIR=${2?Directory to store index entries}

${CDX_INDEX_CLIENT}/cdx-index-client.py -p 16 -c all rottentomatoes.com/m/*  -d ${OUTDIR}

if [[ $? != "0"]]; then
    echo "Error fetching indexes...  Exiting..."
    exit 1 
fi

cat ${OUTDIR}/prefix-rottentomatoes.com*  | sed 's/^.* {/{/g' |  jq -r '[.filename, .offset, .length] | join(" ")'  >  ${OUTDIR}/rottentomatoes.com.lines

