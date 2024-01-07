import argparse
def get_emails(path,name,column,column_name):
    import concurrent.futures
    import pandas as pd
    import time
    from urllib.parse import urlparse
    import string
    from unidecode import unidecode
    pd.options.mode.chained_assignment = None
    import os
    import threading
    from deep_translator  import GoogleTranslator
    from langdetect import detect
    semaphore = threading.Semaphore(10)
    def remove_connecting_words(text):
        connecting_words_in_names = ['de', 'la', 'el', 'del', 'y', 'e', 'i', 'van', 'von', 'di', 'da', 'della', 'le', 'du', 'des', 'et', 'bin', 'ibn', 'al', 'abd', 'als', 'zu', 'zum', 'zur', 'dos', 'das', 'do', 'der', 'den', 'dem', 'und']
        words = text.split()
        filtered_words = [word for word in words if word.lower() not in connecting_words_in_names]
        return ' '.join(filtered_words)

    def translate_to_english(text):
        try:
            if detect(text) != 'en':
                return GoogleTranslator(source='auto', target='en').translate(text)
            else:
                return text
        except:
            return text

    def remove_punctuation(text):
        return remove_connecting_words(translate_to_english(text).translate(str.maketrans('', '', string.punctuation)))

    def get_domain_name(url):
        parsed_uri = urlparse(url)
        domain = '{uri.netloc}'.format(uri=parsed_uri)
        if domain.startswith('www.'):
            domain = domain[4:]
        return domain


    def permutator(first_name, last_name, domain):
        email_list = []
        first_name = first_name.lower()
        last_name = last_name.lower()
        if(last_name == ''):  # 0.1%
            email_list.append(first_name + "@" + domain)
            return email_list
        email_list.append(first_name + "@" + domain)  # 49%
        email_list.append(first_name[0]+last_name + "@" + domain)  # 12.2%
        email_list.append(first_name + "." + last_name + "@" + domain)  # 11.5%
        email_list.append(last_name + "@" + domain)  # 9.6%
        email_list.append(first_name + last_name + "@" + domain)  # 4.6%
        email_list.append(last_name + first_name[0] + "@" + domain)  # 3.5%    
        email_list.append(first_name[0] + "." + last_name + "@" + domain)  # 3.1%
        email_list.append(first_name + last_name[0] + "@" + domain)  # 2.5%
        email_list.append(last_name + first_name + "@" + domain)  # 1.5%
        email_list.append(last_name + "." + first_name + "@" + domain)  # 0.3%
        return email_list

    def verify_single_email(email):
        import requests
        url = f"https://api.listclean.xyz/v1/verify/email/{email}"
        headers = {"X-AUTH-TOKEN": 'API-KEY'}
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            print(email)
            print(response.json()['data']['status'])
            print(response.json()['data']['remarks'])
            return response.json()
        else:
            return None


    def verify_business_emails(first_name, last_name, domain):
        email_list = permutator(remove_punctuation(unidecode(first_name)), remove_punctuation(unidecode(last_name)), get_domain_name(domain))
        unknown = []
        for i in range(len(email_list)):
            verified = verify_single_email(email_list[i])
            if(verified['data']['remarks'] == "MX server not reachable"):
                return '---',i+1,'---'
            if verified['data']['status'] == "clean":
                validEmail = email_list[i]
                return validEmail,i+1,'---'
            if verified['data']['remarks'] == "Catch all Domain":
                unknown.append(email_list[i])
                return '---',i+1,unknown
        if(unknown == []):
            return '---',i+1,'---'
        return '---',i+1,unknown
    
    while(os.path.exists(path+'\\mid.csv')==False):
        time.sleep(3)

    df = pd.read_csv(path+'\\mid.csv')
    df1 = pd.read_csv(path+'\\'+name+'.csv')
    df1.fillna('---', inplace=True)
    df['website'] = '---'
    for i in range(len(df)):
        for j in range(i,len(df1)):
            if(df['company'][i]==df1[column_name][j]):
                df['website'][i] = df1[column][j]
                break
    df['email'] = '---'
    df['em_retry'] = 0
    df['em_status'] = '---'
    df['em_unknown'] = '---'
    
    
    if(os.path.exists(path+'\\finish.csv')==True):
        print("finish.csv exists")
        df_mirror = pd.read_csv(path+'\\finish.csv')
        if(len(df_mirror)<len(df)):  
            for i in range(len(df_mirror)):
                df['email'][i] = df_mirror['email'][i]
                df['em_retry'][i] = df_mirror['em_retry'][i]
                df['em_status'][i] = df_mirror['em_status'][i]
                df['em_unknown'][i] = df_mirror['em_unknown'][i]
                df['website'][i] = df_mirror['website'][i]
        else:
            df = df_mirror
        
    df.to_csv(path+'\\finish.csv', index=False, encoding='utf-8-sig')

    df_lock = threading.Lock()
    def verify_email(i):
        try:
            limit = 3
            if(df['em_status'][i]=='---' and df['website'][i]!='---' and df['name'][i]!='---'):
                unknown = []
                unknowne,emaile,retrye = [],[],[]
                for j,elem in enumerate(df['name'][i].split('\n')):
                    if(j>=limit*2):
                        break
                    if(j%2!=0):
                        continue
                    if(len(elem.split(' '))>1):
                        email, retry,unknown = verify_business_emails(elem.split(' ')[0], elem.split(' ')[1], df['website'][i])
                    else:
                        email, retry,unknown = verify_business_emails(elem.split(' ')[0], '', df['website'][i])            
                    emaile.append(email)
                    retrye.append(retry)   
                    unknowne.append(unknown)
                print(emaile,retrye,unknowne)
                for l in range(len(emaile)-limit):
                    emaile.append('---')
                    retrye.append('---')
                    unknowne.append('---')
                df['email'][i] = "\n".join(emaile)
                df['em_retry'][i] = "\n".join(map(str, retrye))
                df['em_unknown'][i] = "\n".join(map(str, unknowne))
                df['em_status'][i] = 1
                if(df['em_unknown'][i].find('- - -')!=-1):
                    df['em_unknown'][i] = '---'
                
                with df_lock:
                    df_csv = pd.read_csv(path+'\\finish.csv')
                    for elems in range(len(df_csv)):    
                        if(df_csv['company'][i] == df_csv['company'][elems]):
                            df_csv['email'][elems] = df['email'][i]
                            df_csv['em_retry'][elems] = df['em_retry'][i]
                            df_csv['em_status'][elems] = df['em_status'][i]
                            df_csv['em_unknown'][elems] = df['em_unknown'][i]
                    df_csv.to_csv(path+'\\finish.csv', index=False, encoding='utf-8-sig')
        except Exception as e:
            with open(path+'\\error.txt', 'a') as f:
                f.write(str(e))
        semaphore.release()
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            for i in range(len(df)):
                semaphore.acquire()
                executor.submit(verify_email, i)
            for _ in range(10):
                semaphore.acquire()
    finally:
        executor.shutdown(wait=False)        

    return df
    
def main(path, name, column, column_name):
    import time
    import os
    import pandas as pd

    # name = "All"
    # column = "website"
    while True:
        time.sleep(2)
        df = get_emails(path, name, column, column_name)
        if os.path.exists(path+'\\done1.txt'):
            df = get_emails(path, name, column,column_name)
            df_csv = pd.read_csv(path+'\\finish.csv')
            df_csv['em_status'] = 1 
            df_csv.to_csv(path+'\\finish.csv', index=False, encoding='utf-8-sig')
            break
    
    # df = df.loc[:, ["company", "website", "name", "rank", "OwnerLinkedin", "email"]]
    # df = df.rename(columns={"OwnerLinkedin": "linkedin"})
    # df.to_csv(path+'\\client.csv', index=False, encoding='utf-8-sig')
    print("???????????????????????????????????????????FINISHED GETTING EMAILS???????????????????????????????????????????")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("path")
    parser.add_argument("name")
    parser.add_argument("column_names")
    parser.add_argument("column_name")
    args = parser.parse_args()
    
    main(args.path, args.name, args.column_names, args.column_name)
