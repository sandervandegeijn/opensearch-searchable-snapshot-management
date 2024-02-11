import json
import time
from settings import Settings

class Ilm:

    def __init__(self, settings:Settings) -> None:
        self.indices_local = []
        self.indices_remote = []
        self.indices_to_be_snapshotted = []
        self.indices_to_be_restored = []
        self.indices_and_snapshots_to_be_deleted = []
        self.snapshots = []
        self.number_of_days_on_cluster = 7
        self.number_of_days_total_retention = 185
        
        self.requests = settings.get_requests_object()
        self.base_url = settings.get_base_url()        
            
    def index_determine_state(self):
        response = self.requests.get(f"{self.base_url}/_cat/indices?format=json")
        if response.status_code == 200:
            indices_json = json.loads(response.text)
            for index in indices_json:
                    if ("log" in index['index'] or "alert" in index['index']) and  "backup" not in index['index']:

                        #Get settings to determine if index is local or already remote
                        response = self.requests.get(f"{self.base_url}/{index['index']}/_settings")
                        if response.status_code == 200:
                            index_settings = json.loads(response.text)
                            index_creation_timestamp = int(index_settings[index['index']]["settings"]["index"]["creation_date"]) / 1000

                            if "store" in index_settings[index['index']]["settings"]["index"] and index_settings[index['index']]["settings"]["index"]["store"]["type"] == "remote_snapshot":
                                #It's an index with remote storage
                                print(f"Index {index['index']} is a remote snapshot index")
                                self.indices_remote.append(index['index'])
                            else:
                                #It's an index within the cluster
                                print(f"Index {index['index']} is a local index")
                                self.indices_local.append(index['index'])

                                #If the index is old, it should be snapshotted and restored as an index with remote storage
                                if time.time() - index_creation_timestamp >= self.number_of_days_on_cluster * 24 * 3600:
                                    print(f"Index {index['index']} should be snapshotted")
                                    self.indices_to_be_snapshotted.append(index['index'])
                                else:
                                    print(f"Index {index['index']} is not old enough to be snapshotted")

                            #If the index is older than what we need for retention: delete it
                            if time.time() - index_creation_timestamp >= self.number_of_days_total_retention * 24 * 3600:
                                self.indices_and_snapshots_to_be_deleted.append(index['index'])
                                print(f"Index and snapshot of {index['index']} is old and should be deleted")
        else:
            print(f"Could get _cat/indices status code: {response.status_code}")

    def snapshot_determine_state(self):
        response = self.requests.get(f"{self.base_url}/_cat/snapshots/data?v&s=endEpoch&format=json")
        if response.status_code == 200:
            snapshots_json = json.loads(response.text)
            for snapshot in snapshots_json:
                #If a snapshot has succeeded but hasn't been restored already add it to the list
                if snapshot["status"] == "SUCCESS" and snapshot["id"] not in self.indices_remote and "backup" not in snapshot["id"]:
                    print(f"Snapshot {snapshot['id']} should be restored")
                    self.indices_to_be_restored.append(snapshot["id"])
                else:
                    print(f"Snapshot {snapshot['id']} has already been restored")
        else:
            print(f"Could not get snapshots status code: {response.status_code}")
     
    def snapshot_indices(self):
        print(f"Starting snapshotting indices. Number of snapshots/indices: {len(self.indices_to_be_snapshotted)}")

        for index in self.indices_to_be_snapshotted:
            request_data = {
                            "indices": [f"{index}"]
                            }
            response = self.requests.put(f"{self.base_url}/_snapshot/data/{index}", data=json.dumps(request_data))
            if response.status_code == 200:
                print(f"Creating snapshot {index}")
            elif response.status_code == 400:
                print(f"Snapshot {index} already exists but has not been removed/restored")
            else:
                print(f"Error creating snapshot {index} status code: {response.status_code}")

    def remove_index_and_restore_snapshot(self):
        print(f"Starting removing indices and restore remote snapshots. Number of snapshots/indices: {len(self.indices_to_be_restored)}")

        for index in self.indices_to_be_restored:
            response = self.requests.delete(f"{self.base_url}/{index}")
            if response.status_code == 200:
                print(f"Deleted index {index}")
            else:
                print(f"Could not delete old index {index} status code: {response.status_code}")
            
            root = {
            "indices" : index,
            "storage_type" : "remote_snapshot",
            "index_settings": {
                "number_of_replicas": 0
                }
            }
            response = self.requests.post(f"{self.base_url}/_snapshot/data/{index}/_restore", data=json.dumps(root))
            if response.status_code == 200:
                print(f"Restoring snapshot {index}")
            else:
                print(f"Could not restore snapshot {index} status code: {response.status_code} response: {response.text}")
            
    
    def delete_old_indices(self):
        print(f"Deleting {len(self.indices_and_snapshots_to_be_deleted)} old indices")
        
        for index in self.indices_and_snapshots_to_be_deleted:
            print(f"Deleting index {index} because it is old")
            response = self.requests.delete(f"{self.base_url}/{index}")
            if response.status_code == 200:
                print(f"Index {index} deleted")
            else:
                print(f"Could not delete index {index} status code: {response.status_code}")
    
    def delete_old_snapshots(self):
        print(f"Deleting {len(self.indices_and_snapshots_to_be_deleted)} old snapshots")

        for index in self.indices_and_snapshots_to_be_deleted:
            print(f"Deleting snapshot {index} because it is old")
            response = self.requests.delete(f"{self.base_url}/_snapshot/data/{index}")
            if response.status_code == 200:
                print(f"Snapshot {index} deleted")
            else:
                print(f"Could not delete snapshot {index} status code: {response.status_code}")