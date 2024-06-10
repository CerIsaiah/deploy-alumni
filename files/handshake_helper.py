import requests
from urllib.parse import urlencode, quote
import re
import pandas as pd
import time
from concurrent.futures import ThreadPoolExecutor

payload = {}
headers = {
    'accept': 'application/json, text/javascript, */*; q=0.01',
    'accept-language': 'en-US,en;q=0.9',
    'cookie': 'ABTasty=uid=z8cm3088tk3h9xx9&fst=1712946386015&pst=-1&cst=1712946386015&ns=1&pvt=1&pvis=1&th=; _mkto_trk=id:390-ZTF-353&token:_mch-joinhandshake.com-1712946388428-57076; _biz_uid=1908c135d373445db95b59d55d03f4ca; _biz_nA=3; _biz_flagsA=%7B%22Version%22%3A1%2C%22ViewThrough%22%3A%221%22%2C%22XDomain%22%3A%221%22%2C%22Mkto%22%3A%221%22%7D; _biz_pendingA=%5B%5D; ajs_user_id=32289528; ajs_anonymous_id=1e862b26-9f60-46c5-9397-3e5e08da6eac; __pdst=b2d823acc73a4cd0a21630e1dcb7a7c0; production_current_user=32289528; _gcl_au=1.1.1540815463.1712946388.1784160537.1714455470.1714455469; _ga_4M16ZMP2G5=GS1.1.1714507417.5.1.1714509294.0.0.0; _ga=GA1.1.610597308.1712946389; _ga_DXYK6FPBHZ=GS1.1.1715459218.6.1.1715459917.0.0.0; production_submitted_email_address=eyJfcmFpbHMiOnsibWVzc2FnZSI6IkltbGpaWEoyWlc1QVkyOXNaMkYwWlM1bFpIVWkiLCJleHAiOiIyMDQ0LTA1LTI5VDAyOjE5OjM0LjQxNFoiLCJwdXIiOm51bGx9fQ%3D%3D--a46db7b4bfc3711cac87975797ecc6df57f459fe; hss-global=eyJhbGciOiJkaXIiLCJjdHkiOiJKV1QiLCJlbmMiOiJBMjU2Q0JDLUhTNTEyIiwidHlwIjoiSldUIn0..Z-ayX6fbMftfcNiLoBC8dA.mcveAkuACBxNbTYF1oFfh4sd4GjauoGhiRZ6EZrUAu5XZYx39xkvk_DtChGaEMAQZJIbEL9YJFy4AUF1C7gr6VD0dxCSdrzKji5ly9dmlpWeTMTQ8jhge_qQP6aTpZbztp51EWYrq54qfSTPSJlWmHOcLMdaYM75zt_7xBtxsX-fUicwjggZTcBMY2Syt3A0SCeRYWot9LaelYR5FUc1ZqnVMSccl78FJ6x3kxFFmwgL56H9_NLpDpSCu56e9KP-wcLMQ2qwdOEqybh_UXYuPtktdnJhrPArSr8H-AvRP57AyOvm1aRH0Dar1ANWYrM8-p7UKehx7CQ1mC6n_zQZLxGK-xWpqwdBqU6bld6TDhdrHEfHYmRT-PCFMIUW-Ret.-S79pfxQkLggJvrTu0TCDpPY4dNeTtsAVUFWteqpQNg; production_js_on=true; production_32289528_incident-warning-banner-show=%5B%5D; request_method=POST; _trajectory_session=T2xPZmhGU1NhdWJOUkxlanQrdUg4R1hIUTJDVU02REdhR2EyY1dmZnkyeTRSK3ZjWWlNcjYyQVpMUUs5NG9ya08rbGxnd3k1QktiREtITXR4cWh4amdod0drZkIyN2t0emVCd3g5WXVzUVZiSlNBc0ZidUhDR0pJUUduSDBtdnF2cjJnY3V2ZGRkU3A2eVY4NFB6UzlaeUlpY1BmcDZMTndVRXpnYnk5YmVyOXh5WkxBVE1VQWdlbjZtbGQvOGZhLS1Xc1dIS2NRV0VCSWs2VWFjTjBDdDN3PT0%3D--a2270e791164efa143a422ba6eedf70a9963fb7b; _trajectory_session=Rm9QQTVpbGRRT3I1K2NDQ1VRc3ZEbjNUQ2tWMzA5TVRESm5qYWJNRlVzNmJKcDJKdVk4cmhoUjlEMHJrdENwd3BoNldPb3Y2V2dKRklpU0NKS25IQnVCaHpoVWtQZ25OdUtyNHdwbm1XbHB0cURsdy93ZDNDdlhjZ05pV1RjL3VPNm1DbEFDblFxNFdYZ2dRMmhPRWVaM1dwa3ZXYWJvRG1UeFIzTG1adUFJbHBidnRSM3JzMU93MmFFT016WGhxLS1RSTY0MWxNL3pTamtaTmk3SUtpY3JnPT0%3D--ca2d0819b2ebe38a6949ed426eee88970017caf0',
    'priority': 'u=1, i',
    'referer': 'https://colgate.joinhandshake.com/stu/postings?page=2&per_page=25&sort_direction=desc&sort_column=default',
    'sec-ch-ua': '"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
    'x-csrf-token': 'scyEL/Lp28Tsa5m4kulIBMCsFLssFIkhbhVEfDms454eMyEi9lItdXbzJQCsOWk5GccBoSPedBHTqGebB7P/Bg==',
    'x-requested-with': 'XMLHttpRequest'
}



def get_all_id_types(data):
    job_ids = []
    for result in data.get("results", []):
        job_ids.append(result['job_id'])
    return job_ids

def sync_fetch_job_info(url, headers):
    response = requests.get(url, headers=headers)
    if 'application/json' not in response.headers['Content-Type']:
        print(f"Unexpected content type: {response.headers['Content-Type']} for URL: {url}")
        return None

    try:
        response_json = response.json()
    except ValueError as e:
        print(f"ContentTypeError: {e} for URL: {url}")
        return None
    job_info = {
        "Job Type": "Handshake",
        "title": response_json['job']['title'],
        "company": response_json['job']['employer']['name'],
        "description": re.sub('<[^<]+?>', '', response_json['job']['description']).strip(),
        "pay": response_json['job']['pay_rate'],
        "job_url_direct": f"{response_json['job']['employer']['website']}"
    }
    return job_info
#{response_json['job']['job_apply_setting']['external_url']} is the external url for the job
def fetch_job_info(url, headers):
    job_info = sync_fetch_job_info(url, headers)
    return job_info

def get_descriptions(save_ids):
    with ThreadPoolExecutor(max_workers=20) as executor:
        tasks = [executor.submit(fetch_job_info, f"https://colgate.joinhandshake.com/stu/jobs/{str(id)}/search_preview?is_automatically_selected=true&_=1716954917292", headers) for id in save_ids]
        job_info_list = [task.result() for task in tasks]
    
    job_info_list = [job for job in job_info_list if job is not None]
    df = pd.DataFrame(job_info_list)
    return df

def search_handshake(url):
    response = requests.get(url, headers=headers)
    try:
        get_response_json = response.json()
    except ValueError as e:
        print(f"ValueError: {e} for URL: {url}")
        return []  # Return empty list on error
    
    save_ids = get_all_id_types(get_response_json)
    return save_ids

def construct_url(category="Posting", ajax=True, including_all_facets_in_searches=True, page=1, per_page=50, sort_direction="desc", sort_column="created_at", job_type=None, employment_type=None, query="", timestamp="1716993158803"):
    base_url = "https://colgate.joinhandshake.com/stu/postings"
    params = {
        "category": category,
        "ajax": str(ajax).lower(),
        "including_all_facets_in_searches": str(including_all_facets_in_searches).lower(),
        "page": page,
        "per_page": per_page,
        "sort_direction": sort_direction,
        "sort_column": sort_column,
        "_": timestamp
    }
    
    query_params = [
        ("category", params["category"]),
        ("ajax", params["ajax"]),
        ("including_all_facets_in_searches", params["including_all_facets_in_searches"]),
        ("page", params["page"]),
        ("per_page", params["per_page"]),
        ("sort_direction", params["sort_direction"]),
        ("sort_column", params["sort_column"]),
    ]
    
    if job_type is not None:
        query_params.append(("job.job_types%5B%5D", job_type))
    elif employment_type is not None:
        query_params.append(("employment_type_names%5B%5D", employment_type))
    
    query_params.append(("query", quote(query)))
    query_params.append(("_", params["_"]))

    url = base_url + "?" + "&".join([f"{key}={value}" for key, value in query_params])
    return url

# Example usage
url = construct_url(query="marketing", employment_type="Full-Time")  # For full-time jobs
print(url)
# url = construct_url(query="marketing", job_types=3)
# save_ids = search_handshake(url)
# df = get_descriptions(save_ids)
# print(df)
