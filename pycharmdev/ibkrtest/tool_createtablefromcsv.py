
DATADIR='data'                      # this directory name


PREFIX='jobd'
DBNAME='divacsv'

function createSchema {
    COLUMNS=`head -n 1 $1 |
        awk -F, '{for(i=1; i<=NF; i++){out=out $i" text, ";} print out;}' |
        sed 's/ text, $/MYEXTRA text/' |
        sed 's/"//g'`
    CMD_CREATE="psql $DBNAME -c \"CREATE TABLE $2 ($COLUMNS);\""

    echo $CMD_CREATE
    sh -c "$CMD_CREATE"

    CMD_COPY="psql divacsv  -c \"COPY $2 FROM '"`pwd`"/$1' DELIMITER ',' CSV;\""
    echo $CMD_COPY
    sh -c "$CMD_COPY"
}

for file in $DATADIR/*.csv; do
    table=$PREFIX"_"`echo $file | sed 's/.*\///' | sed 's/.csv//' `
    createSchema "$file" $table
done


