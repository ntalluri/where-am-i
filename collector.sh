#!/bin/bash 
# untar file
tar -xzf  GeoLite2-City_20240618.tar.gz

# run
python collector.py

# clean
rm -r GeoLite2-City_20240618
