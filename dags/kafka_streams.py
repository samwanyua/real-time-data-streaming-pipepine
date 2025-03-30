from datetime import datetime 
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'wanyua',
    'start_date': datetime(2025, 3, 31, 8,00),
    'retries': 1,
}

def get_data():
    import json
    import requests
    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    res = res['results'][0]
    # print(json.dumps(res, indent=3))

    return res

def format_data(res):
    data = {}
    location = res['location']
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{location['street']['number']} {location['street']['name']}, {location['city']}, {location['state']}, {location['country']} - {location['postcode']}"
    data['username'] = res['login']['username']
    data['postcode'] = location['postcode']
    data['dob'] = res['dob']['date'][:10]  # Extract YYYY-MM-DD
    data['registered_date'] = res['registered']['date'][:10]  # Extract YYYY-MM-DD
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']
    data['location'] = {
        "street": f"{location['street']['number']} {location['street']['name']}",
        "city": location['city'],
        "state": location['state'],
        "country": location['country'],
        "postcode": location['postcode'],
        "latitude": location['coordinates']['latitude'],
        "longitude": location['coordinates']['longitude']
    }

    return data

def stream_data():
    import json
    res = get_data()
    res = format_data(res)
    print(json.dumps(res, indent=3))


with DAG('user_automation',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False
         ) as dag:
    
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )

stream_data()