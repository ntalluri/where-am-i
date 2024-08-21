"""
TODO-LIST
"""
import ast
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import seaborn as sns
import os 
import numpy as np
import scipy.stats as stats

# check if useable_data dir if exists
directory = 'useable_data'
if not os.path.exists(directory):
    print(f"Directory '{directory}' DNE. Collect data with collector.py to run stats.py")
    exit()

# create stats directory dir if DNE
stats_directory = 'stats'
if not os.path.exists(stats_directory):
    os.makedirs(stats_directory)

# create sub dirs in stats_ irectory dir if DNE
stat_plots = [f'{stats_directory}/stem_plots', f'{stats_directory}/hist_plots', f'{stats_directory}/scatter_plots']
for folder in stat_plots: 
    if not os.path.exists(folder):
        os.makedirs(folder) 

# concat all non-empty df
dataframes = []
num_of_file = 0
for filename in os.listdir(directory):
    date_time = filename.split('.')[1]
    date = date_time.split('_')[0]
    time = date_time.split('_')[1][:5]

    temp_df = pd.read_csv(os.path.join(directory, filename))
    num_of_file+=1
    if not temp_df.empty:
        temp_df["Date"]= date
        temp_df["Time"]= time
        dataframes.append(temp_df)

print(f"number of dataframes: {num_of_file}")
print(f"number of dataframes used: {len(dataframes)}")

main_df = pd.concat(dataframes, ignore_index=True).drop_duplicates(keep="last", ignore_index=True)
sorted_main_df = main_df.sort_values(['Resource Name', 'Mac Address', 'Date','Time'])
sorted_main_df.to_csv(f"{stats_directory}/all_data.csv", index=False, header=True, sep="\t")

# create df of resource name, mac address, ping latency avg , date, and time
grouped_df_ping_avg = main_df.groupby(['Resource Name', 'Mac Address', 'Date', 'Time'])['Ping Latency Avg'].apply(list).reset_index()
grouped_df_ping_avg = grouped_df_ping_avg.explode('Ping Latency Avg')
grouped_df_ping_avg = grouped_df_ping_avg.dropna(subset=['Ping Latency Avg'])

#  create visulizations for each resource name with all associated mac addresses
resources = grouped_df_ping_avg['Resource Name'].unique()
for resource in resources:

    resource_data = grouped_df_ping_avg[grouped_df_ping_avg['Resource Name'] == resource]
    mac_addresses = resource_data['Mac Address'].unique()
    colors = plt.get_cmap(name="tab20b", lut=len(mac_addresses))
    
    # stem polt
    plt.figure(figsize=(10,7))

    for idx, mac in enumerate(mac_addresses, start=1):
        mac_data = resource_data[resource_data['Mac Address'] == mac]
        color = colors(idx - 1)
        markerline, stemlines, baseline = plt.stem([idx] * len(mac_data), mac_data['Ping Latency Avg'], linefmt='--', label=mac)
        plt.setp(stemlines, color=color)
        plt.setp(markerline, color=color)

    plt.ylabel('Ping Latency Avg')
    plt.xlabel("Mac Address")
    plt.title(f"{resource.upper()}")
    plt.xticks(range(1, len(mac_addresses) + 1), mac_addresses, rotation=90)
    # plt.legend(title="Mac Address", bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    plt.savefig(f"{stats_directory}/stem_plots/resource-{resource}-stem.png")
    plt.close()
    
    # histogram
    plt.figure(figsize=(10,7))
    for idx, mac in enumerate(mac_addresses):
        mac_data = resource_data[resource_data['Mac Address'] == mac]
        color = colors(idx)
        plt.hist(mac_data['Ping Latency Avg'], alpha = 0.3, bins= np.arange(0, 100, 1), label=mac, color=color, edgecolor='black')

    plt.ylabel('Frequency')
    plt.xlabel('Ping Latency Avg')
    plt.title(f"{resource.upper()}")
    plt.legend(title="Mac Address", bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    plt.savefig(f"{stats_directory}/hist_plots/resource-{resource}-histogram.png")
    plt.close()

    # scatter plot
    plt.figure(figsize=(10, 7))
    for idx, mac in enumerate(mac_addresses, start=1):
        mac_data = resource_data[resource_data['Mac Address'] == mac]
        color = colors(idx - 1)
        plt.scatter([idx] * len(mac_data), mac_data['Ping Latency Avg'], color=color, label=mac)
    
    plt.ylabel('Ping Latency Avg')
    plt.xlabel("Mac Address")
    plt.title(f"{resource.upper()}")
    plt.xticks(range(1, len(mac_addresses) + 1), mac_addresses, rotation=90)
    # plt.legend(title="Mac Address", bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    plt.savefig(f"{stats_directory}/scatter_plots/resource-{resource}-scatter.png")
    plt.close()


# create statistic and outlier report
def zscore(x):
    return list((x - x.mean()) / x.std())

def check_outlier(z_scores):
    return any(np.abs(z) > 2 for z in z_scores)

# stats per resource
resource_stats = grouped_df_ping_avg.groupby(['Resource Name']).agg(
    mean_latency=('Ping Latency Avg', 'mean'),
    median_latency=('Ping Latency Avg', 'median'),
    variance_latency=('Ping Latency Avg', 'var'),
    stddev_latency=('Ping Latency Avg', 'std'),
    latency_25th_percentile=('Ping Latency Avg', lambda x: x.quantile(0.25)),
    latency_75th_percentile=('Ping Latency Avg', lambda x: x.quantile(0.75)),
    latency_list=('Ping Latency Avg', lambda x: list(x)),
).reset_index()

order = ['Resource Name', 'mean_latency','median_latency', 'variance_latency', 'stddev_latency', 'latency_25th_percentile', 'latency_75th_percentile', 'latency_list']
column_names = ['Resource_Name', 'Mean','Median', 'Variance', 'Standard_Deviation', '25th_Percentile', '75th_Percentile', 'Latencys']

resource_stats.fillna(value="None", inplace=True)
resource_stats = resource_stats[order]
resource_stats.columns = column_names
resource_stats.to_csv(f"{stats_directory}/resource-stats.csv", index=False, header=True, sep="\t")

# outliers per resource
order = ['Resource Name', 'mac_address_list','date_list', 'time_list', 'latency_list','z_score_latency']
column_names = ['Resource_Name', 'Mac_Address', 'Date', 'Time', 'Latency','Z_Score']

resource_zscore_stats = grouped_df_ping_avg.groupby(['Resource Name']).agg(
    z_score_latency=('Ping Latency Avg', lambda x: zscore(x)),
    mac_address_list = ('Mac Address', lambda x:list(x)),
    latency_list=('Ping Latency Avg', lambda x: list(x)),
    date_list = ('Date', lambda x: list(x)),
    time_list = ('Time', lambda x: list(x)),
).reset_index()

resource_outliers = resource_zscore_stats[resource_zscore_stats['z_score_latency'].apply(check_outlier)].reset_index(drop=True)
resource_outliers = resource_outliers.explode(["mac_address_list", 'z_score_latency', 'latency_list', 'date_list', 'time_list'])
resource_outliers = resource_outliers[resource_outliers['z_score_latency'].apply(lambda z: np.abs(z) > 2)]
resource_outliers = resource_outliers[order]
resource_outliers.columns = column_names
resource_outliers.sort_values(['Resource_Name', 'Mac_Address', 'Date','Time'], inplace=True)
resource_outliers.to_csv(f"{stats_directory}/resource-outliers.csv", index=False, header=True, sep="\t")


# # stats per resource and associated mac addresses; not useful now, but could be in the future
# resource_macaddy_stats = grouped_df_ping_avg.groupby(['Resource Name', 'Mac Address']).agg(
#     mean_latency=('Ping Latency Avg', 'mean'),
#     median_latency=('Ping Latency Avg', 'median'),
#     variance_latency=('Ping Latency Avg', 'var'),
#     stddev_latency=('Ping Latency Avg', 'std'),
#     latency_25th_percentile=('Ping Latency Avg', lambda x: x.quantile(0.25)),
#     latency_75th_percentile=('Ping Latency Avg', lambda x: x.quantile(0.75)),
#     latency_list=('Ping Latency Avg', lambda x: list(x)),
# ).reset_index()

# order = ['Resource Name', 'Mac Address','mean_latency','median_latency', 'variance_latency', 'stddev_latency', 'latency_25th_percentile', 'latency_75th_percentile', 'latency_list']
# column_names = ['Resource Name', 'Mac Address','Mean','Median', 'Variance', 'Standard Deviation', '25th Percentile', '75th Percentile', 'Latencys']

# resource_macaddy_stats.fillna(value="None", inplace=True)
# resource_macaddy_stats = resource_macaddy_stats[order]
# resource_macaddy_stats.columns = column_names
# resource_macaddy_stats.to_csv(f"{stats_directory}/resource-macaddress-stats.csv", index=False, header=True, sep="\t")


# # outliers per resources and associated mac addresses
# order = ['Resource Name', 'Mac Address','date_list', 'time_list', 'latency_list','z_score_latency']
# column_names = ['Resource Name', 'Mac Address', 'Date', 'Time', 'Latency','Z Score']

# resource_macaddy_zscore_stats = grouped_df_ping_avg.groupby(['Resource Name', 'Mac Address']).agg(
#     z_score_latency=('Ping Latency Avg', lambda x: zscore(x)),
#     mac_address_list = ('Mac Address', lambda x:list(x)),
#     latency_list=('Ping Latency Avg', lambda x: list(x)),
#     date_list = ('Date', lambda x: list(x)),
#     time_list = ('Time', lambda x: list(x)),
# ).reset_index()

# resource_macaddy_outliers = resource_macaddy_zscore_stats[resource_macaddy_zscore_stats['z_score_latency'].apply(check_outlier)].reset_index(drop=True)
# resource_macaddy_outliers = resource_macaddy_outliers.explode(["mac_address_list", 'z_score_latency', 'latency_list', 'date_list', 'time_list'])
# resource_macaddy_outliers = resource_macaddy_outliers[resource_macaddy_outliers['z_score_latency'].apply(lambda z: np.abs(z) > 2)]
# resource_macaddy_outliers = resource_macaddy_outliers[order]
# resource_macaddy_outliers.columns = column_names
# resource_macaddy_outliers.sort_values(['Resource Name', 'Date','Time'], inplace=True)
# resource_macaddy_outliers.to_csv(f"{stats_directory}/resource-macaddress-outliers.csv", index=False, header=True, sep="\t")