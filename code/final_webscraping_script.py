import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import concurrent.futures
import glob


headers = {
    'User-Agent': 'Mozilla/5.0 (Linux; Android 5.1.1; SM-G928X Build/LMY47X) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.83 Mobile Safari/537.36'}


def get_state_abbr():
    url = 'https://www.ssa.gov/international/coc-docs/states.html'
    page = requests.get(url=url, headers=headers)
    soup = BeautifulSoup(page.content, "html.parser")
    table = soup.find("table")
    
    state, state_abb = [],[]
    for i, field in enumerate(table.find_all('td',class_="grayruled-td")):
        if i%2 == 0:
            state.append(field.text.strip())
        else:
            state_abb.append(field.text.strip())
    state_abb = {
        'State' : state,
        'State_abb' : state_abb}
        
    df = pd.DataFrame(state_abb)
    removed_state = ['GU','MP','PR','AS']
    df = df[~df['State_abb'].isin(removed_state)]
    df.reset_index(inplace = True, drop = True)
    df.to_csv('scraping/State_abb.txt')

def get_state_link(state:str):
    url = f'https://www.zip-codes.com/state/{state.lower()}.asp'
    baseurl = 'https://www.zip-codes.com'
    page = requests.get(url=url, headers=headers)
    # print(page.text)
    soup = BeautifulSoup(page.content, "html.parser")
    table = soup.find("table", class_="statTable")
    
    state_detail_link = []
    for td in table.find_all("td"):
        name = td.find('a')
        if str(name) == "None":
            continue
        else:
            if 'zip-code' in name['href']:
                state_detail_link.append(baseurl + name['href'])
    return state_detail_link
# print(state_detail_link)


def parse_table(link):
    index_range = [0,1,2,3,14,15]
    state_info = []

    page = requests.get(url=link, headers=headers)
    soup = BeautifulSoup(page.content, "html.parser")
    table = soup.find("table", class_="statTable")

    attributes = []
    for i, attribute in enumerate((table.find_all("span", class_ = "mblTip"))):
        if i in index_range:
            attributes.append(attribute['title'].split("::")[0].split('/')[0].strip())

    fields = []
    for i, field in enumerate(table.find_all('td', class_='info')):
        if i in index_range:
            fields.append(field.text.strip())

    state_zipcode_info = dict(zip(attributes, fields))
    state_info.append(state_zipcode_info)
    df = pd.DataFrame(state_info)
    df['State_Abbr'] = df['State'].apply(lambda x : x.split('[')[0])
    df['State'] = df['State'].apply(lambda x : x.split('[')[1])
    df['State'] = df['State'].apply(lambda x : re.sub(r'[\W_]+',' ',x))
    df['Counties'] = df['Counties'].apply(lambda x : x.split(',')[0])
    df['State FIPS'] = df['State FIPS'].apply(lambda x : str(x).zfill(2))
    df['County FIPS'] = df['County FIPS'].apply(lambda x : str(x).zfill(3))   
    state = str(df['State'].unique()[0]).strip()
    df.to_csv(f'scraping/{state}.csv', mode = 'a', header = False, index= False)

def main():
    with open('scraping/State_abb.txt','r') as f:
        lines = f.readlines()[1:]
        for line in lines:
            state = line.split(',')[2].split('\n')[0]
            with concurrent.futures.ThreadPoolExecutor() as executor:
                executor.map(parse_table,get_state_link(state))


def combined_csv():
    extension = 'csv'
    all_filenames = [i for i in glob.glob('scraping/*.{}'.format(extension))]
    #combine all files in the list
    combined_csv = pd.concat([pd.read_csv(f, header = None) for f in all_filenames ], axis = 0)
    #export to csv
    combined_csv.columns = ['ZIP Code', 'City', 'State', 'Counties', 'State FIPS', 'County FIPS','State_Abbr']
    combined_csv.to_csv("combined_csv.csv", index=False, encoding='utf-8-sig')

# print(get_state_link('AK'))
# parse_table(get_state_link('AK'),'AK')



# get_state_abbr()
# main()
# combined_csv()

# check 
df = pd.read_csv("combined_csv.csv")
assert len(df['State_Abbr'].unique()) == 52

