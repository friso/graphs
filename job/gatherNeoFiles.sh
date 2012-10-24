#!/bin/bash

rm -rf ./graph.db
mkdir graph.db/

TO=./graph.db/
FROM=${1}
hadoop fs -get ${FROM}/neostore ${TO}
hadoop fs -get ${FROM}/neostore.id ${TO}
hadoop fs -get ${FROM}/neostore.nodestore.db.id ${TO}
hadoop fs -get ${FROM}/neostore.relationshipstore.db.id ${TO}
hadoop fs -get ${FROM}/neostore.relationshiptypestore.db ${TO}
hadoop fs -get ${FROM}/neostore.relationshiptypestore.db.id ${TO}
hadoop fs -get ${FROM}/neostore.relationshiptypestore.db.names ${TO}
hadoop fs -get ${FROM}/neostore.relationshiptypestore.db.names.id ${TO}

hadoop fs -get ${FROM}/properties/neostore.propertystore.db.* ${TO}

hadoop fs -cat ${FROM}/neostore.nodestore.db/part-r-* > ${TO}/neostore.nodestore.db
hadoop fs -cat ${FROM}/neostore.relationshipstore.db/part-r-* > ${TO}/neostore.relationshipstore.db

hadoop fs -cat ${FROM}/nodeproperties/propertystore.db/props-r-* ${FROM}/edgeproperties/propertystore.db/props-r-* ${FROM}/properties/neostore.propertystore.db.footer > ${TO}/neostore.propertystore.db
hadoop fs -cat ${FROM}/properties/neostore.propertystore.db.strings.header ${FROM}/nodeproperties/propertystore.db/strings-r-* ${FROM}/edgeproperties/propertystore.db/strings-r-* ${FROM}/properties/neostore.propertystore.db.strings.footer > ${TO}/neostore.propertystore.db.strings

rm ${TO}/*.footer
rm ${TO}/*.header
exit
