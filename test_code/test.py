import os
from datetime import datetime
import pandas as pd

def main():   

    id_file_path = 'test_ids.csv'
    # get the current day 11:59 pm 
    death = datetime.now().replace(hour=23, minute=00, second=0, microsecond=0)
    # age = datetime.now().replace(hour= 11, minute=30, second=1, microsecond=0)
    print(death)

    # https://stackoverflow.com/questions/8142364/how-to-compare-two-dates

    if os.path.exists(id_file_path):
        file_stats = os.stat(id_file_path)
        birth_time = file_stats.st_ctime
        print(os.stat(id_file_path))
        age = datetime.now()
        # age = datetime.fromtimestamp(birth_time)
        print(age) 
        if age > death:
            print("old")
            os.remove(id_file_path)
            collector_ids = pd.DataFrame(columns=["Glidein ID"])
        else:
            print("young")
            collector_ids = pd.read_csv(id_file_path)
    else:
        print("DNE")
        collector_ids = pd.DataFrame(columns=["Glidein ID"])

    df = pd.read_csv("test_collector_data/collector.2024-08-05_15:10:11PM.csv")
    unique_df = df[~df["Glidein ID"].isin(collector_ids["Glidein ID"])]
    updated_collector_ids = pd.concat([collector_ids["Glidein ID"], unique_df["Glidein ID"]], ignore_index=True)
    updated_collector_ids.to_csv(id_file_path, index=False)

    # uncomment parts to test different functionality

    # unique_df = df[~df["Glidein ID"].isin(collector_ids["Glidein ID"])]
    # updated_collector_ids = pd.concat([collector_ids["Glidein ID"], unique_df["Glidein ID"]], ignore_index=True)
    # updated_collector_ids.to_csv(id_file_path, index=False)

    # df = pd.read_csv("test_collector_data/collector.2024-08-05_15:10:19PM.csv")
    # unique_df = df[~df["Glidein ID"].isin(collector_ids["Glidein ID"])]
    # updated_collector_ids = pd.concat([collector_ids["Glidein ID"], unique_df["Glidein ID"]], ignore_index=True)
    # updated_collector_ids.to_csv(id_file_path, index=False)

    # df = pd.read_csv("test_collector_data/collector.2024-08-05_15:17:47PM.csv")
    # unique_df = df[~df["Glidein ID"].isin(collector_ids["Glidein ID"])]
    # updated_collector_ids = pd.concat([collector_ids["Glidein ID"], unique_df["Glidein ID"]], ignore_index=True)
    # updated_collector_ids.to_csv(id_file_path, index=False)

    # unique_df = df[~df["Glidein ID"].isin(collector_ids["Glidein ID"])]
    # updated_collector_ids = pd.concat([collector_ids["Glidein ID"], unique_df["Glidein ID"]], ignore_index=True)
    # updated_collector_ids.to_csv(id_file_path, index=False)


if __name__ == "__main__":
    main()