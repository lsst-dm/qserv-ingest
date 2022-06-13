 ls -l /usr/share/nginx/html/datasets/case01/partition/case01/Source/ | egrep "chunk.*\.txt" | grep -v overlap | awk '{print $9}' |  sed "s/chunk_//" | sed "s/_overlap//" | sed "s/\.txt/,/"
