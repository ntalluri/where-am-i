#!/bin/bash 
# untar file
tar -xzf  binaries/GeoLite2-City_20240618.tar.gz

# run
python src/collector.py

# clean
rm -r GeoLite2-City_20240618
