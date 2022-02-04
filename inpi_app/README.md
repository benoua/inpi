# Locally 

```
docker build -t inpi_scrapper --file inpi_app/Dockerfile .
```


#App deployment 

## Cloud RUN version (choosen)
https://medium.com/google-cloud/deploying-containers-to-cloud-run-in-5mins-b03f1d8d4a64

```
gcloud builds submit --tag gcr.io/eighth-feat-304320/inpi-image . && gcloud run deploy inpi-scrapper --image gcr.io/eighth-feat-304320/inpi-image --region europe-west1 --platform managed --allow-unauthenticated --quiet
```

## Compute Engine version

TODO :
https://minimaxir.com/2018/11/cheap-cron/
https://kiosk-dot-codelabs-site.appspot.com/codelabs/cloud-picadaily-lab3/index.html?index=..%2F..index#0


## GKE version
#https://medium.com/hashmapinc/how-to-build-and-deploy-docker-images-on-gke-part-ii-f8466922f5f5
gcloud container clusters create inpi-cluster --num-nodes=1 --machine-type=n1-standard-1 --zone=europe-west1-b

kubectl create deployment inpi-cluster —-image=gcr.io/eighth-feat-304320/inpi-image

kubectl create -f deployment.yaml


## FileZila download

Upload data to gs (benoit project )
```
gcloud config set project glossy-topic-333719
gsutil rsync -r /Users/bletournel/Documents/perso/cairn/data/inpi_ftp/Bilans_Donnees_Saisies/flux gs://cairn-bucket/Bilans_Donnees_Saisies/flux/
```

```
gcloud config set project eighth-feat-304320
gsutil rsync -r /Users/bletournel/Documents/perso/cairn/data/inpi_ftp/Bilans_Donnees_Saisies/flux gs://cairn-bucket-1/Bilans_Donnees_Saisies/flux/
```

Create a VM on vertex notebook 

```console
mkdir /home/jupyter/Bilans_Donnees_Saisies
gsutil rsync -r gs://cairn-bucket-1/Bilans_Donnees_Saisies/ /home/jupyter/Bilans_Donnees_Saisies/
python unzip_all.py --mode=prod_remote && sudo shutdown -h now
```


LOAD data to SQL server

Create Table with client 
gcloud sql import csv cairn-db gs://cairn-bucket/dodo_dodo_inpi_to_load.csv --database=inpi --table=financials --quote="22" --escape="5C" --fields-terminated-by="2C" --lines-terminated-by="0A" 

