"""
TODO-LIST
"""

import htcondor
import pandas as pd
from collections import Counter
from datetime import datetime
from resourcename import get_topology_resource_data
import subprocess
import json 
import os
import statistics as st

COLLECTOR = "cm-1.ospool-itb.osg-htc.org"


def main():

    # create useable_data dir if DNE
    directory = 'useable_data'
    if not os.path.exists(directory):
        os.makedirs(directory)

    # getting topology data
    topology = get_topology_resource_data()

    # gather glideins that are partitionalble and certain attributes from ads
    projection = [
        "WhereAmIToCache", "GLIDEIN_Site", "GLIDEIN_ResourceName", "TracePathOutput", "PublicIPV4", "PublicIPV6", 
        "HasTCPPing", "TCPPingOutput","Hostnames" ,"HostnamesIPs","NetworkInterfaces",
        "VIRTUALIZATION_TECHNOLOGY", "GLIDEIN_SiteWMS_Queue", "GLIDEIN_SiteWMS", "OSG_OS_KERNEL",
        "HardwareAddress", "Microarch", "OSG_CPU_MODEL", "DetectedCpus", "DetectedGPUs", "DetectedMemory", 
        "GLIDEIN_MASTER_NAME"
    ]
    version = "chick.6" # verison of where-am-i glidein script
    constraint = f'WhereAmIVersion == "{version}" && (SlotType == "Partitionable" || SlotType == "Static")'
    coll = htcondor.Collector(COLLECTOR)
    ads = coll.query( 
        htcondor.AdTypes.Startd,
        projection=projection,
        constraint=constraint, 
    )

    # create/open file to hold all unqiue Glidein ID's (GLIDEIN_MASTER_NAME)
    id_file_path = 'collector_ids.csv'
    death = datetime.now().replace(hour=23, minute=30, second=0, microsecond=0)
    if os.path.exists(id_file_path):
        age = datetime.now()
        if age >= death:
            print("collector_ids is old")
            os.remove(id_file_path)
            collector_ids = pd.DataFrame(columns=["Glidein ID"])
        else:
            print("collector_ids is young")
            collector_ids = pd.read_csv(id_file_path)
    else:
        print("collector_ids DNE")
        collector_ids = pd.DataFrame(columns=["Glidein ID"])
    

    # init df
    columns_df = [
    'Cache Name', 'Institution','Site', 'Resource Name', 'Mac Address',
    'Public IPV4', 'Latitude ipv4', 'Longitude ipv4', 'Accuracy ipv4', 
    'Public IPV6', 'Latitude ipv6', 'Longitude ipv6', 'Accuracy ipv6',
    'TracePath Hops', 'TracePath Latency (ms)', 'TracePath IPs', 
    'Ping Latency Min', 'Ping Latency Avg', 'Ping Latency Max', 'Ping Latency Mdev',
    'Host Names', 'Host Names IPs', 'Network Interfaces',
    "Container Type", "Batch System", "Batch System CM", "Linux Kernel", 
    "CPU Capabilities", "CPU kind", "Detected CPUs", "Detected GPUs", "Detected Memory", 
    "Glidein ID"]

    df = pd.DataFrame(columns=columns_df)

    # create a row in df that represents one ad/glidein
    for ad in ads:
        # get "easy" attributes
        cache = (ad['WhereAmIToCache'])
        site = (ad['GLIDEIN_Site']).lower()
        resource = (ad['GLIDEIN_ResourceName']).lower()
        pub_ip4 = (ad['PublicIPV4'])
        pub_ip6 = (ad['PublicIPV6'])
        container = (ad['VIRTUALIZATION_TECHNOLOGY'])
        batch_sys = (ad['GLIDEIN_SiteWMS'])
        batch_sys_cm = (ad['GLIDEIN_SiteWMS_Queue'])
        mac_addy = (ad['HardwareAddress'])
        cpu_kind = (ad['OSG_CPU_MODEL'])
        cpu_ability = (ad['Microarch'])
        cpus = (ad['DetectedCpus'])
        gpus= (ad['DetectedGPUS'])
        memory= (ad['DetectedMemory'])
        lin_kernel = (ad['OSG_OS_KERNEL'])
        hostname = (ad['Hostnames'])
        hostname_ip = (ad['HostnamesIPs'])
        network_interfaces = (ad['NetworkInterfaces'])
        glidein_id = (ad['GLIDEIN_MASTER_NAME'])

        # get institution name from resource name
        try:
            institution = topology[resource]['institution']
        except Exception as e:
            institution = "None" 
        
        # get latitude, longitude, and accuracy ipv4
        db_path='GeoLite2-City_20240618/GeoLite2-City.mmdb'

        mmdbinspect_process = subprocess.Popen(
            ['binaries/mmdbinspect', '-db', db_path, pub_ip4],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        mmdbinspect_output, mmdbinspect_error = mmdbinspect_process.communicate()
        jq_process = subprocess.Popen(
            ['binaries/jq-linux-amd64', '.[].Records? // [] | map(select(.Record.location.latitude != null and .Record.location.longitude != null) | {latitude: .Record.location.latitude, longitude: .Record.location.longitude, accuracy: .Record.location.accuracy_radius}) | .[]'],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        jq_output, jq_error = jq_process.communicate(input=mmdbinspect_output)
        if jq_output.decode().strip() == "": # usually happens when using a public ip
            lat_ipv4 = "None"
            longi_ipv4 = "None"
            acc_ipv4 = "None"
        else:
            locations = json.loads(jq_output.decode())
            lat_ipv4 = locations.get('latitude')
            longi_ipv4 = locations.get('longitude')
            acc_ipv4 = locations.get('accuracy')

        # get latitude, longitude, and accuracy ipv6
        db_path='GeoLite2-City_20240618/GeoLite2-City.mmdb'

        mmdbinspect_process = subprocess.Popen(
            ['binaries/mmdbinspect', '-db', db_path, pub_ip6],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        mmdbinspect_output, mmdbinspect_error = mmdbinspect_process.communicate()
        jq_process = subprocess.Popen(
            ['binaries/jq-linux-amd64', '.[].Records? // [] | map(select(.Record.location.latitude != null and .Record.location.longitude != null) | {latitude: .Record.location.latitude, longitude: .Record.location.longitude, accuracy: .Record.location.accuracy_radius}) | .[]'],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        jq_output, jq_error = jq_process.communicate(input=mmdbinspect_output)
        if jq_output.decode().strip() == "": # usually happens when using a public ip
            lat_ipv6 = "None"
            longi_ipv6 = "None"
            acc_ipv6 = "None"
        else:
            locations = json.loads(jq_output.decode())
            lat_ipv6 = locations.get('latitude')
            longi_ipv6 = locations.get('longitude')
            acc_ipv6 = locations.get('accuracy')

        # set up new row for df
        new_row = {
            'Cache Name': cache,
            'Institution': institution,
            'Site': site,
            'Resource Name': resource,
            'Mac Address': mac_addy,
            'Public IPV4': pub_ip4, 
            'Latitude ipv4': lat_ipv4,
            'Longitude ipv4': longi_ipv4,
            'Accuracy ipv4': acc_ipv4,
            'Public IPV6': pub_ip6,
            'Latitude ipv6': lat_ipv6,
            'Longitude ipv6': longi_ipv6,
            'Accuracy ipv6': acc_ipv6,
            'TracePath Hops': [],
            'TracePath Latency (ms)': [],
            'TracePath IPs': [],
            'Ping Latency Min': "None",
            'Ping Latency Avg': "None",
            'Ping Latency Max': "None",
            'Ping Latency Mdev': "None",
            'Container Type': container,
            'Batch System': batch_sys, 
            'Batch System CM': batch_sys_cm,
            "Linux Kernel": lin_kernel,
            "CPU Capabilities": cpu_ability,
            "CPU kind": cpu_kind, 
            'Detected CPUs': cpus,
            'Detected GPUs': gpus,
            'Detected Memory': memory,
            'Host Names': hostname,
            'Host Names IPs': hostname_ip,
            'Network Interfaces': network_interfaces,
            'Glidein ID': glidein_id,
        }

        # parse ping output 
        ping_output = ad['TCPPingOutput'].split('XbrX')
        
        if 'Unknown' not in ping_output: 
            ping_output = ping_output[1:-1]
            pings = []
            for p in ping_output:
                p_split = p.split(',')
                pings.append(float(p_split[-1]))
            new_row['Ping Latency Min'] = min(pings)
            new_row['Ping Latency Avg'] = max(pings)
            new_row['Ping Latency Max'] = sum(pings) / len(pings)
            new_row['Ping Latency Mdev'] = st.pstdev(pings)

        # parse traceroute output
        tp_output = ad['TracePathOutput'].split('XbrX')
        hops = []
        times = []
        ips = []

        for output in tp_output:
            if 'Unknown' in output: 
                break
            hop_and_details = output.split(":")
            details = hop_and_details[1].split()
            hop_num = hop_and_details[0].strip()

            if ('no' and 'reply') in details or 'reached' in details:
                break
            else:
                if 'asymm' in details:
                    details = details[:-2]
                ip = details[1].strip('()')
                time = details[-1].replace('ms', '')
                
            hops.append(hop_num)
            times.append(time)
            ips.append(ip)

        if hops:
            new_row['TracePath Hops'] = hops[-1]
        if times:
            new_row['TracePath Latency (ms)'] = times[1:]
        if ips:
            new_row['TracePath IPs'] = ips[1:]
        
        # add new row to df
        df.loc[len(df)] = new_row

    # only save new glideins that aren't already in the data using collector_ids file and Glidein ID's from new data
    unique_df = df[~df["Glidein ID"].isin(collector_ids["Glidein ID"])]
    date = datetime.now().strftime("%Y-%m-%d_%H:%M:%S%p")
    unique_df.to_csv(f"{directory}/collector.{date}.csv", index=False) 
    
    # update file with collector_ids
    updated_collector_ids = pd.concat([collector_ids["Glidein ID"], unique_df["Glidein ID"]], ignore_index=True)
    updated_collector_ids.to_csv(id_file_path, index=False)

    print(unique_df)

if __name__ == "__main__":
    main()
