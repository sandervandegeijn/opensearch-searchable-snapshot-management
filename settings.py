import requests

class Settings:

    def __init__(self, url:str, bucket:str, cert_file_path:str, key_file_path:str, number_of_days_on_cluster:int, number_of_days_total_retention:int) -> None:
        self.url = url
        self.bucket = bucket
        self.number_of_days_on_cluster = number_of_days_on_cluster
        self.number_of_days_total_retention = number_of_days_total_retention
        self.cert_file_path = cert_file_path
        self.key_file_path = key_file_path
    
    def get_requests_object(self):
        cert = (self.cert_file_path, self.key_file_path)
        s = requests.Session()
        s.cert = cert
        s.verify = False
        s.headers = {"content-type": "application/json", 'charset':'UTF-8'}
        return s