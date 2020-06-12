 kubectl exec -it qserv-ingest-db-0 -- mysql -h localhost -u root -pCHANGEME -e "select count(*) from qservIngest.task;"
