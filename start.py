import argparse
def collect_data(path,name,column,n): 
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
    max_parallel_executions = 5
    semaphore = threading.Semaphore(max_parallel_executions)
    df = pd.read_csv(path+'\\'+name+'.csv')
    
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

    df_head = pd.DataFrame(columns=['link_data', 'text_data', 'req_retry','gpt_data'])
    for j in range(n):
        df_head.loc[j] = ['---', '---', 0, '---']
    df_head['company'] = df[str(column)]
    
    if os.path.isfile(path+'\\start.csv'):
        print("start.csv exists")
        df_copy = pd.read_csv(path+'\\start.csv',usecols=['link_data', 'text_data', 'req_retry','gpt_data','company'])
        if(len(df_copy)<n):   
            for i in range(min(len(df_copy), n)):
                df_head['link_data'][i] = df_copy['link_data'][i]
                df_head['text_data'][i] = df_copy['text_data'][i]
                df_head['req_retry'][i] = df_copy['req_retry'][i]
                df_head['gpt_data'][i] = df_copy['gpt_data'][i]
        else:
            df_head = df_copy
    df_head.to_csv(path+'\\start.csv', index=False,encoding='utf-8-sig')
    df = df_head
    time.sleep(3)
    
    def process_data(i):
        if(df['req_retry'][i]>7 or df['text_data'][i]!='---'):
            semaphore.release() 
            return
        link = '---'
        print(i,". ",df['company'][i])
        query = """site:linkedin.com/in/ ~owner"""
        query1 = """~owner"""
        
        link = google_search_url(query+f""" "{df['company'][i]}" """)
        ok = 0
        while df['req_retry'][i] < 4 and ok == 0:
            try:
                response = client.get(link, params=params)
                json_data = json.loads(response.text)
                duh = len(json_data['organic_results'])
                ok = 1
            except Exception as e:
                with open(path+'\\errorsEncounteredReq.txt', 'a') as file:
                    file.write(str(e) + '\n at' +str(i)+ '\n\n')
                df['req_retry'][i] += 1
                print(i,". ",df['company'][i], "retrying", df['req_retry'][i])
  
        try:
            if ok==1 and duh and unidecode(df['company'][i].lower().replace('.',' ')) in unidecode(f"{json_data['organic_results'][0]['title'].replace('-', ' ').replace('·', '->')}->{json_data['organic_results'][0]['description'].replace('-', ' ').replace('·', '->')}".lower().replace('.',' ')):
                df['text_data'][i] = response.text
            else:
                if(df['req_retry'][i]<4):
                    df['req_retry'][i] = 4
                link = google_search_url(query1+f""" "{df['company'][i]}" """)
                ok = 0
                while df['req_retry'][i] < 8 and ok == 0:
                    try:
                        response = client.get(link, params=params)
                        json_data = json.loads(response.text)
                        duh = len(json_data['organic_results'])
                        ok = 1
                    except Exception as e:
                        with open(path+'\\errorsEncounteredReq.txt', 'a') as file:
                            file.write(str(e) + '\n')
                        df['req_retry'][i] += 1
                        print(i,". ",df['company'][i], "retrying", df['req_retry'][i])  
                if ok==1 and duh and unidecode(df['company'][i].lower().replace('.',' ')) in unidecode(f"{json_data['organic_results'][0]['title'].replace('-', ' ').replace('·', '->')}->{json_data['organic_results'][0]['description'].replace('-', ' ').replace('·', '->')}".lower().replace('.',' ')):
                    df['text_data'][i] = response.text
                else:
                    df['text_data'][i] = '---'
                    df['req_retry'][i] = 8
        except Exception as e:
            with open(path+'\\errorsEncountered.txt', 'a') as file:
                file.write(str(e) + '\n')

        df['link_data'][i] = link
        with df_lock:
            df_head = pd.read_csv(path+'\\start.csv',encoding='utf-8-sig')
        
            df_head['link_data'][i] = df['link_data'][i]
            df_head['text_data'][i] = df['text_data'][i]
            df_head['req_retry'][i] = df['req_retry'][i]
            df_head['company'][i] = df['company'][i]
            
            if(df_head['text_data'][i]!='---'):
                df_head['gpt_data'][i] = parse_data(df_head['text_data'][i])
            df_head.to_csv(path+'\\start.csv', index=False,encoding='utf-8-sig')  
        semaphore.release()  
            
    # with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
    #     for i in range(n):
    #         executor.submit(process_data, i)
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_parallel_executions) as executor:
            for i in range(n):
                semaphore.acquire()
                executor.submit(process_data, i)

            for _ in range(max_parallel_executions):
                semaphore.acquire()
    finally:
        # Close the executor
        executor.shutdown(wait=False)

def main(path,name,column,n):
    collect_data(path, name, column, n)
    with open(path+'\\done.txt', 'w') as f:
        pass
    print("???????????????????????????????????????????FINISHED COLLECTING DATA???????????????????????????????????????????")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("path")
    parser.add_argument("name")
    parser.add_argument("column")
    parser.add_argument("n", type=int)
    args = parser.parse_args()
    main(args.path, args.name, args.column, args.n)
    
    