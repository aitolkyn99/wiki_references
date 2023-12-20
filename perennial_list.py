import pandas as pd 
import requests 
from bs4 import BeautifulSoup
import pickle


SAVE_PATH = 'data/output/'


def get_source_table(table):
    '''
    Function to parse perennial source table to a pandas dataframe 
    '''
    records = []
    columns = ['name','status','List','Last','Summary','source']
    for tr in table.findAll("tr"):
        ths = tr.findAll("th")
        if ths != []:
            continue
        else:
            trs = tr.findAll("td")
            record = []
            urls = []
            for i, each in enumerate(trs):
                try:
                    if 'Special:Search/insource' in each.a['title']:
                        for a in each.findAll('a'):
                            if 'Special:Search/insource' in a['title']:
                                link = a['href']
                                link = link.replace('/wiki/Special:Search/insource:%22','').replace('%22','')
                                urls.append(link)
                    elif i == 1:
                        text = each.a['title']
                        record.append(text)
                    else:
                        text = each.text.replace('\n','')
                        record.append(text)
                except:
                    if len(record)==5:
                        continue
                    else:
                        text = each.text
                        record.append(text)
            for url in urls:
                tmp = record.copy()
                tmp.append(url)
                records.append(tmp)

            # Three special cases loaded previously
            if len(urls)==0 and record[0]=='Peerage websites (self-published)':
                for url in peerage:
                    tmp = record.copy()
                    tmp.append(url)
                    records.append(tmp)
            if record[0] == 'Advameg (City-Data)':
                for url in advameg:
                    tmp = record.copy()
                    tmp.append(url)
                    records.append(tmp)
            elif record[0] == 'Natural News (NewsTarget)':
                for url in newstarget:
                    tmp = record.copy()
                    tmp.append(url)
                    records.append(tmp)
            
    df = pd.DataFrame(data = records, columns = columns)
    df = df[df.source.str.contains('\.')] # this is a simple check for broken urls; May need more rigorous check depending on the list version
    return df


wikiurl="https://en.wikipedia.org/wiki/Wikipedia:Reliable_sources/Perennial_sources" #link to the English Wiki's perennial sources list
table_class="wikitable sortable perennial-sources jquery-tablesorter"
response=requests.get(wikiurl)
soup = BeautifulSoup(response.text, 'html.parser')
table=soup.find('table',{'class':"wikitable"})

# Currently URLs that belong to "Advameg", "Natural News (NewsTarget)", and "Peerage websites" are listed separately. So, I collect them separately in the 'data' directory
# Below I load the lists

advameg = []
newstarget = []
peerage = [] 

with open('data/advameg.pkl', 'rb') as f:
    advameg = pickle.load(f)
with open('data/newstarget.pkl', 'rb') as f:
    newstarget = pickle.load(f)
with open('data/peerage.pkl', 'rb') as f:
    peerage = pickle.load(f)


perennials_df = get_source_table(table)
perennials_df.to_csv(SAVE_PATH+'enwiki_perennial_list.csv', index=False)