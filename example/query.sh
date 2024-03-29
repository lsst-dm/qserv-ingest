#!/bin/bash
# Lauch queries against a Qserv instance

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR"/../env.sh

CZAR_HOST="${INSTANCE}-czar-0.${INSTANCE}-czar"
# DATABASE="dc2_object_run2_2i_dr6_wfd"
# DATABASE="cosmoDC2_v1_1_4_image_test"
# DATABASE="cosmoDC2_v1_1_4_image_overlap"
DATABASE="dc2_run2_1i_dr1b"
DATABASE="dp01_dc2_catalogs"

#time kubectl exec -it $INSTANCE-czar-0 -- \
#    bash -lc ". /qserv/stack/loadLSST.bash && setup mariadb && mysql --socket /qserv/data/mysql/mysql.sock --user=root --pass=CHANGEME -e 'select count(*) from qservMeta.${DATABASE}__position;'"
time kubectl exec -it $INSTANCE-ingest-db-0 -- \
    bash -lc "mysql --host $CZAR_HOST --port 4040 --user qsmaster -e 'SELECT * FROM $DATABASE.position LIMIT 10'"
time kubectl exec -it $INSTANCE-ingest-db-0 -- \
    bash -lc "mysql --host $CZAR_HOST --port 4040 --user qsmaster -e 'SELECT COUNT(*) FROM $DATABASE.position'"
#time kubectl exec -it $INSTANCE-ingest-db-0 -- \
#    bash -lc "mysql --host $CZAR_HOST --port 4040 --user qsmaster -e \
#    'SELECT objectId, coord_ra, coord_dec FROM $DATABASE.position as p, $DATABASE.dpdd_ref as r WHERE p.objectId=r.objectId LIMIT 10'"
