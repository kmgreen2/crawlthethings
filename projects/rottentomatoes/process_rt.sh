BASEDIR=`dirname $0`/../..
INDEX_ENTRY_FILE=${1?File containing the index entries}
OUTDIR=${2?Output directory for the results}

pipenv run python3 ${BASEDIR}/src/main.py -t 32 -i ${INDEX_ENTRY_FILE} -o ${OUTDIR} -p rottentomatoes

if [[ $? != "0"]; then
    echo "Error running processor.  Exiting..."
    exit 1
fi

for prefix in 0 1 2 3 4 5 6 7 8 9 a b c d e f; do
    cat ${OUTDIR}/results.${prefix}*.json | jq -r '.[]' | \
        jq -r '[(.ts|tostring),.uri, .criticScore, .numCritic, .audienceScore, .numAudience] | join(",")' | \
        sed -E 's/https?:\/\/(www\.)?rottentomatoes.com\/m\/([^\/]*)\/?/\2/'  | \
        awk -F, '{split($2,ary,"?"); print $1","ary[1]","$3","$4","$5","$6;}' | \
        grep -v null >> ${OUTDIR}/movies.csv
done

echo "create table rt(ts bigint, name varchar, critic smallint, numCritic smallint, audience smallint, numAudience smallint);" | sqlite3 ${OUTDIR}/movies.db

cat ${OUTDIR}/movies.csv | awk -F, -f ${BASEDIR}/projects/rottentomatoes/insert_stmt.awk | sqlite3 ${OUTDIR}/movies.db


