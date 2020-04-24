kubectl exec -it qserv-ingest-db-0 -- mysql -h localhost -u root -pCHANGEME -e "update qservIngest.task set status=NULL, pod=NULL"
