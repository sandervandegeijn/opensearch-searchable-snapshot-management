from snapshot import Snapshot
from settings import Settings
from ilm import Ilm
import argparse
import urllib3

#Disaster recovery   
def disaster_recovery():
    print.info("Recovering cluster to latest snapshot!")
    snapshot = Snapshot(settings)
    latest_snapshot = snapshot.get_latest_snapshot()
    print(f"Latest snapshot: {latest_snapshot}")

    snapshot.restore_snapshot(latest_snapshot)

#Snapshots
def snapshot_list():
    snapshot = Snapshot(settings)
    snapshot.get_snapshots()

def snapshot_restore(number_of_days):
    snapshot = Snapshot(settings)
    snapshot.restore_snapshot(number_of_days)

def snapshot_restore_latest():
    snapshot = Snapshot(settings)
    snapshot.restore_snapshot(snapshot.get_latest_snapshot())

def ilm():
    ilm = Ilm(settings)
    ilm.index_determine_state()
    ilm.snapshot_determine_state()
    ilm.snapshot_indices()
    ilm.remove_index_and_restore_snapshot()
    ilm.delete_old_indices()
    ilm.delete_old_snapshots()

if __name__ == "__main__":
    #Pesky self signed certs
    urllib3.disable_warnings()
    parser = argparse.ArgumentParser(description="Scripting to modify and maintain Opensearch")

    choices={
             "snapshot-list",
             "snapshot-restore",
             "snapshot-restore-latest",
             "ilm"
             }

    parser.add_argument('-action',required=True,choices=choices,help="What do you want me to do?")
    parser.add_argument("-url", required=True, help="The base-URL of this opensearch cluster (https://opensearch-master-nodes:9200)")
    parser.add_argument("-bucket", required=True, help="The name of the S3 bucket")
    parser.add_argument('-snapshotname', required=False, help="snapshot name from snapshot-list to restore a specific snapshot")
    parser.add_argument("-number_of_days_on_cluster", required=False, default=60, help="Number of days that the data will be available on the cluster as a searchable snapshot")
    parser.add_argument("-number_of_days_total_retention", required=False, default=10, help="Number of days that the data will be available on the cluster as a searchable snapshot")
    parser.add_argument("-cert_file_path", required=True, help="Path to the public key (\"../assets/certs/admin.pem\")")
    parser.add_argument("-key_file_path", required=True, help="Path to the private key (\"../assets/certs/admin-key.pem\")")
    args = parser.parse_args()

    action = args.action
    snapshotname = args.snapshotname
    url = args.url
    bucket = args.bucket

    settings = Settings(url, bucket, args.cert_file_path, args.key_file_path, args.number_of_days_on_cluster, args.number_of_days_total_retention)
    
    if not action:
        print ("No options given")
        parser.print_usage()
        exit(1)
    
    elif action == "snapshot-list": snapshot_list()
    elif action == "snapshot-restore": snapshot_restore(snapshotname)
    elif action == "snapshot-restore-latest": snapshot_restore_latest()
    elif action == "ilm": ilm()
    else:
        print ("No correct options given")
        parser.print_usage()
        exit(1)