import json
from settings import Settings


class Snapshot:

    def __init__(self, settings:Settings) -> None:
        self.base_url = settings.get_base_url()
        self.bucket_name = settings.bucket
        self.indices = [".kibana*", 
                        ".opensearch-sap-pre-packaged-rules-config", 
                        ".plugins-ml-config", 
                        ".opensearch-observability", 
                        ".opensearch-notifications-config", 
                        ".opensearch-sap-log-types-config"]
        self.requests = settings.get_requests_object()
        self.register_bucket()

    def register_bucket(self) -> bool:
        response = self.requests.get(self.base_url+"/_snapshot/disaster-recovery")
        if response.status_code == 404:
            print("s3 repo not initialized - registering")
            payload = {
                    "type": "s3",
                    "settings": {
                        "bucket": self.bucket_name,
                        "compress": True
                        }
                    }
            response = self.requests.put(self.base_url+"/_snapshot/disaster-recovery", data=json.dumps(payload))
            if response.status_code == 200:
                print("Registering s3 repo successful")
                return True
            else:
                print("Registering s3 repo failed")
                print(str(response.text))
                return False

    def restore_snapshot(self, snapshot_id:str) ->bool:
        #Deleting old index before restore
        for index in self.indices:
            print(f"Deleting index {index}")
            response = self.requests.delete(f"{self.base_url}/{index}")
            print(f"Statuscode: {response.status_code}")
        
        root = {
                "indices" : self.indices,
                "include_global_state" : False
                }

        response = self.requests.post(self.base_url+"/_snapshot/disaster-recovery/"+snapshot_id+"/_restore", data=json.dumps(root))
        if response.status_code == 200:
            print(f"Restoring snapshot {snapshot_id} request: \n {json.dumps(root)}")
            return True
        else:
            print("Restoring snapshot failed "+snapshot_id)
            print(str(response.text))
            return False
    
    def get_snapshots(self) -> bool:
        response = self.requests.get(self.base_url+"/_cat/snapshots/disaster-recovery?v&s=endEpoch")
        if response.status_code == 200:
            print("Snapshots:")
            print("\n" + response.text)
            return True
        else:
            print("Listing snapshots failed")
            print(response.text)
            return False
    
    def get_latest_snapshot(self) -> str:
        response = self.requests.get(self.base_url+"/_cat/snapshots/disaster-recovery?v&s=endEpoch&format=json")
        if response.status_code == 200:
            response_json = json.loads(response.text)
            return response_json[len(response_json)-1]["id"]