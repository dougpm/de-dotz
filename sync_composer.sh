#!/bin/bash

cd de-dotz
git pull https://github.com/dougpm/de-dotz
gsutil -m rsync -d -r /home/douglas_martins/de-dotz/ gs://southamerica-east1-composer-9872da8a-bucket/dags/dotz
