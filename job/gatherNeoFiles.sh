#!/bin/bash

rm -rf ./graph.db
mkdir graph.db/

TO=./graph.db/
FROM=${1}
hadoop fs -get ${FROM}/neostore* ${TO}
hadoop fs -get ${FROM}/properties/neostore.propertystore.db.* ${TO}

hadoop fs -cat ${FROM}/neostore.nodestore.db/part-r-* > ${TO}/neostore.nodestore.db
hadoop fs -cat ${FROM}/neostore.relationshipstore.db/part-r-* > ${TO}/neostore.relationshipstore.db

hadoop fs -cat ${FROM}/properties/propertystore.db/props-r-* ${FROM}/properties/neostore.propertystore.db.footer > ${TO}/neostore.propertystore.db
hadoop fs -cat ${FROM}/properties/neostore.propertystore.db.strings.header ${FROM}/properties/propertystore.db/strings-r-* ${FROM}/properties/neostore.propertystore.db.strings.footer > ${TO}/neostore.propertystore.db.strings

rm ${TO}/*.footer
rm ${TO}/*.header
exit
