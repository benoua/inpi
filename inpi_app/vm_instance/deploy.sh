MY_INSTANCE_NAME="my-app-instance"
ZONE=europe-west1-b

gcloud compute instances create $MY_INSTANCE_NAME \
    --image-family=debian-9 \
    --image-project=debian-cloud \
    --machine-type=g1-small \
    --metadata-from-file startup-script=startup_script.sh \
    --zone $ZONE