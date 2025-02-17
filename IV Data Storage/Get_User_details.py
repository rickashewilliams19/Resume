client_id = " " # Add the account Holder ID
secret_key = " " # Add the Account Holder Secret_key
redirect_uri = " " # Add the redirect url which should match the url at the time of account creation
code = " " # Add the code after running the get daily user code python file

import requests

url = "https://api.upstox.com/v2/login/authorization/token"

headers = {
  'Content-Type': 'application/x-www-form-urlencoded',
  'Accept': 'application/json'
}

Data = {
    'code': code,
    'client_id' : client_id,
    'client_secret' : secret_key,
    'redirect_uri' : redirect_uri,
    'grant_type' : 'authorization_code',
}


response = requests.post(url, headers= headers, data= Data)

print(response.status_code)
print(response.json())


access_token = response.json()['access_token']
print(access_token)