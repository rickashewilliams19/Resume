client_id = " " # Add the account Holder ID
secret_key = " " # Add the Account Holder Secret_key
redirect_uri = " " # Add the redirect url which should match the url at the time of account creation

url = f'https://api.upstox.com/v2/login/authorization/dialog?response_type=code&client_id={client_id}&redirect_uri={redirect_uri}'

print(url)
