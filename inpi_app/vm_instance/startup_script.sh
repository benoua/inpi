# Install or update needed software
apt-get update
apt-get install -yq git supervisor python python-pip
pip install --upgrade pip virtualenv

echo "Making directories..."
mkdir -p /home/benoit/rawdata
mkdir -p /home/benoit/code

# Fetch source code
export HOME=/root
#git clone https://github.com/GoogleCloudPlatform/getting-started-python.git /opt/app
#git clone https://source.developers.google.com/projects/eighth-feat-304320/repos/cairn /opt/app
git clone https://github.com/benoua/inpi.git  /home/benoit/inpi
cd /home/benoit/inpi/inpi_app
pip install -r requirements.txt
python3 unzip_all.py

# sync file


#sudo pip install --upgrade google-cloud
#sudo pip install --upgrade google-cloud-storage
#sudo pip install --upgrade google-api-python-client
#sudo pip install --upgrade google-auth-httplib2


