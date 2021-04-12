#!/bin/bash

set -euxo pipefail

echo "Delete unattached disks"
gcloud compute disks delete $(gcloud compute disks list --filter="-users:*" --format "value(name)")
