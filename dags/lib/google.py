import requests

def google_search(api_key, cx, query, num=5):
    search_url = "https://www.googleapis.com/customsearch/v1"
    params = {
        "key": api_key,
        "cx": cx,
        "q": query,
        "num": num
    }
    response = requests.get(search_url, params=params, timeout=20)
    response.raise_for_status()
    search_results = response.json()
    return search_results.get('items', [])