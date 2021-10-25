
argo submit   --serviceaccount=argo-workflow -p 'workflow-table-name=objectTable_tract' -p 'prefix=PREOPS-863'  manifests/preingest.yaml
argo submit   --serviceaccount=argo-workflow -p 'workflow-table-name=sourceTable_visit' -p 'prefix=PREOPS-863'  manifests/preingest.yaml
argo submit   --serviceaccount=argo-workflow -p 'workflow-table-name=forcedSourceTable' -p 'prefix=PREOPS-863'  manifests/preingest.yaml
