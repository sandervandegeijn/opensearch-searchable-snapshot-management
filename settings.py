import requests

class Settings:

    def __init__(self, url, bucket) -> None:
        self.url = url
        self.bucket = bucket

    def get_base_url(self): 
        return self.url

    def get_bucket_name(self):
        return self.bucket
    
    def get_requests_object(self):
        cert_file_path = "../assets/certs/admin.pem"
        key_file_path = "../assets/certs/admin-key.pem"
        cert = (cert_file_path, key_file_path)
        s = requests.Session()
        s.cert = cert
        s.verify = False
        s.headers = {"content-type": "application/json", 'charset':'UTF-8'}
        return s