import requests
r = requests.post('http://127.0.0.1:8080/stores/add', json={"address": "MGIMO University", "region":11})

print(r.status_code)