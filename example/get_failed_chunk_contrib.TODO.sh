# Get not-finished chunk contrib
kubectl exec -it  qserv-repl-db-0 -c repl-db -- mysql -pCHANGEME qservReplica -e 'SELECT * FROM `transaction_contrib` WHERE end_time=0'

# Get status for this ones
kubectl exec -it  qserv-repl-db-0 -c repl-db -- mysql -pCHANGEME qservReplica -e 'SELECT transaction_id,chunk,num_rows,success FROM `transaction_contrib` WHERE id IN (2752,2748)'
