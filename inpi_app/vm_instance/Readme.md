
gcloud compute instances create test-bl --image-project=debian-cloud --image-family=debian-10 --metadata-from-file startup-script=startup_script.sh --zone=europe-west1-b --machine-type=g1-small

####  view output 
sudo journalctl -u google-startup-scripts.service