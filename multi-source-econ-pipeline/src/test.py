import requests, json

api_key = "HDR-vTYwQJysQkhZiwm7UkWlqvMc57FecC0p"
url = f"https://hdrdata.org/api/CompositeIndices/query?apikey={api_key}&countryOrAggregation=USA&indicator=hdi,le,mys"
response = requests.get(url, timeout=30)
print(response.status_code)
print(json.dumps(response.json()[:3], indent=2))