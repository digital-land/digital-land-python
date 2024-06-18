# install most recent version of GDAL on linux, remove when apt catches up
sudo apt-get install build-essential python-all-dev
wget https://github.com/OSGeo/gdal/releases/download/v3.9.0/gdal-3.9.0.tar.gz
tar xvfz gdal-3.9.0.tar
cd gdal-3.9.0
./configure --with-python
make
sudo make install