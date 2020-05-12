
import requests
import sys
url='http://lsst-qserv-master01:25080/ingest/v1/chunk'
transactionId=102
database='desc_dc2_run12p_v4e'
f = open('unique_chunk_numbers')
for chunkStr in f:
    chunk = int(chunkStr)
    response = requests.post(url, json={"transaction_id":transactionId,"chunk":chunk,"auth_key":""})
    responseJson = response.json()
    if not responseJson['success']:
        print("failed for chunk: %d, error: %s" % (chunk,responseJson['error'],))
        sys.exit(1)
    else:
        host = responseJson['location']['host']
        port = responseJson['location']['port']
        print("%d %s %d" % (chunk,host,port))
