import argparse
def get_emails(df,website):
    import concurrent.futures
    import pandas as pd
    import sys
    import time
    from urllib.parse import urlparse
    import string
    from unidecode import unidecode
    pd.options.mode.chained_assignment = None
    import os
    import threading
    def remove_connecting_words(text):
        connecting_words_in_names = ['de', 'la', 'el', 'del', 'y', 'e', 'i', 'van', 'von', 'di', 'da', 'della', 'le', 'du', 'des', 'et', 'bin', 'ibn', 'al', 'abd', 'als', 'zu', 'zum', 'zur', 'dos', 'das', 'do', 'der', 'den', 'dem', 'und']
        words = text.split()
        filtered_words = [word for word in words if word.lower() not in connecting_words_in_names]
        return ' '.join(filtered_words)


    def remove_punctuation(text):
        return remove_connecting_words(text.translate(str.maketrans('', '', string.punctuation)))

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
            # print(email)
            # print(response.json()['data']['status'])
            # print(response.json()['data']['remarks'])
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
    

    df['website'] = website
    df['email'] = '---'
    df['em_retry'] = 0
    df['em_status'] = '---'
    df['em_unknown'] = '---'

    if(df['website'][0]=='---' or df['name'][0]=='---'):
        df['em_status'] = 1
        return df
    
    while(df['em_status'][0]=='---'):
        unknown = []
        unknowne = []
        for elem in df['name'][0].split('\n'):
            if(len(elem.split(' '))>1):
                email, retry,unknown = verify_business_emails(elem.split(' ')[0], elem.split(' ')[1], df['website'][0])
            else:
                email, retry,unknown = verify_business_emails(elem.split(' ')[0], '', df['website'][0])
            if(email != '---'):
                df['email'][0] = email
                df['em_retry'][0] = retry
                df['em_status'][0] = 1
                break
            unknowne.append(unknown)
        if(df['em_status'][0]=='---'):
            df['em_status'][0] = 1
            df['em_retry'][0] = retry 
            df['em_unknown'][0] = '\n'.join([' '.join(map(str, sublist)) for sublist in unknowne])
            if(df['em_unknown'][0].find('- - -')!=-1):
                df['em_unknown'][0] = '---'
        else:
            df['email'][0] = email
            df['em_retry'][0] = retry
            df['em_status'][0] = 1
    return df
    
def main(df,website):

    df = get_emails(df,website)

    #df = df.loc[:, ["company", "website", "name", "rank", "OwnerLinkedin", "email"]]
    #df = df.rename(columns={"OwnerLinkedin": "linkedin"})
    return df

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("df")
    parser.add_argument("website")
    args = parser.parse_args()
    main(args.df, args.website)
