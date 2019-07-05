#!/bin/bash

#$FLINK_HOME/bin/flink run -c erreesse.query.Query1 /sabd/jar/SABD-project2-application-1.0-SNAPSHOT.jar
PROGNAME=$0

usage() {
cat << EOF >&2
Usage: $PROGNAME -q <queryname>

-q : name of query [query1|query2|query3]
EOF
exit 1
}
# valori di default
#queryname=query1 context=local fileformat=csv

while getopts q:r o; do
case $o in
 (q) queryname=$OPTARG;;
 (*) usage
esac
done
shift "$((OPTIND - 1))"

#echo Remaining arguments: "$@"
wrong_query_name() {
 echo "ERROR: Query name not exists";
 usage
}
# #set query class name
# query_class="erreesse.query.Query1"
case $queryname in
 ("query1") query_class="erreesse.query.Query1";;
 ("query2") query_class="erreesse.query.Query2";;
 ("query3") query_class="erreesse.query.Query3";;
 (*) wrong_query_name
esac

echo "queryname: "$queryname;
echo "query_class: "$query_class;

start_flink_app() {
$FLINK_HOME/bin/flink run \
-c $query_class /sabd/jar/SABD-project2-application-1.0-SNAPSHOT.jar
}

start_flink_app