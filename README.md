# WHERE-AM-I
## PROJECT DESCRIPTION
- TODO

## PIPELINE
1) run collector and sleep jobs. This script will submit a job that uses crondor.

`. submit.sh`

2) run stats code manually when data is generated

`. stats.sh`

## CLEANUP
1) run clean up script when pipeline finishes

`. cleanup.sh`

## FILES
### submit.sh
- creates logs directory
- submits 50 crondor sleep jobs every three hours
- submits 1 crondor collector job every 30 minutes
### collector.sh
- runs collector.py src code 
### collector.py
- collects data on glidein instances that meet specific version and slot type criteria. The data is organized into a DataFrame and stored in CSV files within a directory, ensuring only new entries are added based on unique identifiers. Additionally, the script handles file management by creating directories, removing outdated data, and maintaining a log (collector_ids.csv) of processed glidein IDs to prevent redundant data collection.
### stats.sh
- runs stats.py src code
### stats.py
-  The script identifies outliers based on z-scores, generating reports on outlier instances for further investigation. It organizes data into specific directories, creates various plots (stem, histogram, scatter) to visually represent latency data associated with various resources and MAC addresses, and calculates statistical measures including mean, median, and standard deviation.
### resourcename.py
- Retrieves resource data from the OSG Topology service, mapping each resource's name to its institutional information. It handles data fetching with the option to force an update or use cached data if it's less than one day old. 

