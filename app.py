import requests, os
import random
import time

print("Hello world !")

API_URL = os.getenv("API_URL", "http://localhost:8000/")
API_TOKEN = os.getenv("API_TOKEN", "")

headers = {
    "Authorization": f"Bearer {API_TOKEN}", 
    "Content-Type": "application/json"  
}

data = {
    f"key_A": f"value_{random.randint(0, 100)}",  
    f"key_B": f"value_{random.randint(0, 100)}",
}

if __name__=='__main__':

    time.sleep(10)
    try:
        _greet = requests.get(API_URL, json=data, headers=headers)
        print(_greet.json())
    except Exception as e:
        print(f"Error greet : {API_URL} {e}")

    try:
        response = requests.post(f"{API_URL}write/db/testcollection", json=data, headers=headers)
        if response.status_code == 200:
            print("Success:", response.json())
        else:
            print("Error:", response.status_code, response.text)
    except Exception as e:
        print(f"Error post : {API_URL} {e}")
