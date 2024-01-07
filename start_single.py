import argparse
def collect_data(name): 
    import time
    import pandas as pd
    import urllib.parse
    import json
    import concurrent.futures
    from unidecode import unidecode
    pd.options.mode.chained_assignment = None
    from zenrows import ZenRowsClient
    import os
    import threading
    client = ZenRowsClient("API-KEY")
    params = {"premium_proxy":"true","autoparse":"true"}
    df_lock = threading.Lock()
    
    def google_search_url(query):
        query = urllib.parse.quote(query)
        return f'https://www.google.com/search?hl=en&client=firefox-b-d&q={query}'
    def parse_data(input_string):
        try:
            json_data = json.loads(input_string)
            results = json_data.get('organic_results', [])
            return '\n'.join([f"{result['title'].replace('-', ' ').replace('·', '->')}->{result['description'].replace('-', ' ').replace('·', '->')}" for result in results[:5]])
        except:
            return '---'

    df = pd.DataFrame(columns=['link_data', 'text_data', 'req_retry','gpt_data'])
    for j in range(1):
        df.loc[j] = ['---', '---', 0, '---']
    df['company'] = name

    
    for query in ["""site:linkedin.com/in/ ~owner""","""~owner"""]:
        df['req_retry'] = 0
        while df['req_retry'][0]<3 and df['text_data'][0]=='---':
            link = '---'
            link = google_search_url(query+f""" "{df['company'][0]}" """)
            while True:
                try:
                    response = client.get(link, params=params)
                    print(query,'request:',df['req_retry'][0])
                    df['req_retry'][0] += 1
                except:
                    df['req_retry'][0] += 1
                if response.status_code == 200 or df['req_retry'][0]==3:
                    break
            if df['req_retry'][0]==3:
                break
            try:
                json_data = json.loads(response.text)
                duh = len(json_data['organic_results'])
            except:
                break
            if duh:
                if unidecode(df['company'][0].lower()) not in unidecode(f"{json_data['organic_results'][0]['title'].replace('-', ' ').replace('·', '->')}->{json_data['organic_results'][0]['description'].replace('-', ' ').replace('·', '->')}".lower()):
                    df['text_data'][0] = '---'
                else:
                    df['text_data'][0] = response.text
            else:
                df['text_data'][0] = '---'
            df['link_data'][0] = link
            
                
            if(df['text_data'][0]!='---'):
                df['gpt_data'][0] = parse_data(df['text_data'][0])
                break
        if(df['text_data'][0]!='---'):
            break

    return df


def main(name):
    df = collect_data(name)
    return df

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("name")
    args = parser.parse_args()
    main(args.name)
    
