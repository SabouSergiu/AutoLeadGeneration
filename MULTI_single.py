


def get_name_rank_linkedin(df):
    import pandas as pd
    import time
    def check_linkedin(name, linkedins):
        ok = 0
        for linkedin in linkedins:
            for n in name.split(' '):
                if n.lower() in linkedin.lower() and n != '':
                    ok += 1
                    break
            if len(name.split(' ')) > 1:
                if name.split(' ')[1].lower() in linkedin.lower():
                    ok += 1
            if ok > 1:
                break
        if ok > 1:
            return linkedin
        else:
            return '---'
        
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
    def get_domain_name(url):
        from urllib.parse import urlparse
        parsed_uri = urlparse(url)
        domain = '{uri.netloc}'.format(uri=parsed_uri)
        if domain.startswith('www.'):
            domain = domain[4:]
        return domain
    df.reset_index(drop=True, inplace=True) 
    backupname = '---'
    backuplinkedin = '---'
    backuprank = '---'
    mainlinkedin = '---'
    mainname = '---'
    mainrank = '---'
    mainpermutator = '---'
    backuppermutator = '---'

    # print(df.columns)
    if(df['name'][0]!= "---"):
        if(len(df['name'][0].split('\n'))==2):
            mainname = df['name'][0].split('\n')[0]
            mainlinkedin = check_linkedin(df['name'][0].split('\n')[0],df['OwnerLinkedin'][0].split('\n'))
            mainrank = df['rank'][0].split('\n')[0]
            mainpermutator = permutator(df['name'][0].split('\n')[0].split(' ')[0],df['name'][0].split('\n')[0].split(' ')[1],get_domain_name(df['website'][0]))
            
            backupname = df['name'][0].split('\n')[1]
            backuplinkedin = check_linkedin(df['name'][0].split('\n')[1],df['OwnerLinkedin'][0].split('\n'))
            backuprank = df['rank'][0].split('\n')[1]
            backuppermutator = permutator(df['name'][0].split('\n')[1].split(' ')[0],df['name'][0].split('\n')[1].split(' ')[1],get_domain_name(df['website'][0]))
            # print('')
        else:
            mainname = df['name'][0]
            mainlinkedin = check_linkedin(df['name'][0],df['OwnerLinkedin'][0].split('\n'))
            mainrank = df['rank'][0]
            mainpermutator = permutator(df['name'][0].split(' ')[0],df['name'][0].split(' ')[1],get_domain_name(df['website'][0]))
            backupname = '---'
            backuplinkedin = '---'
            backuprank = '---'
            backuppermutator = '---'
            # print('')
    ok = 0
    if(df['email'][0]!="---"):
        if(df['email'][0] in mainpermutator):
            ok = 1
        else:
            if(df['email'][0] in backuppermutator):
                ok = 2
    if(ok == 1):
        return mainname, mainrank, mainlinkedin, df['email'][0]
    if(ok == 2):
        return backupname, backuprank, backuplinkedin, df['email'][0]
    
    return mainname, mainrank, mainlinkedin, df['email'][0]



def main(df):
    import warnings
    warnings.filterwarnings("ignore")
    df = get_name_rank_linkedin(df)
    return df

def main_df(df):
    import warnings
    warnings.filterwarnings("ignore")
    for i in range(len(df)):
        df.name[i],df['rank'][i],df.OwnerLinkedin[i],df.email[i] = get_name_rank_linkedin(df[i:i+1])
    return df

