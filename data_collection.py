# This code is accessing a WMF data lakes table

import pyspark.sql.functions as F
import pyspark.sql.types as T
import re 
import pandas as pd
import wmfdata
spark = wmfdata.spark.create_session(type='yarn-regular')

SAVE_PATH = 'data/output/'

# data collection setup
snapshot = '2023-02'
sources_list_path = SAVE_PATH+'enwiki_perennial_list.csv'

def source_mappings(sources_list_path):
    '''
    map sources from perennial source to cat 'source_cat_dict' and to id 'source_id_dict'
    '''
    sources_list = pd.read_csv(sources_list_path)

    label_codes = {'Generally reliable':5, 'No consensus':4, 'Generally unreliable':3,'Deprecated':2,'Blacklisted':1}
    #mapping the sources labels in codes from 1 to 5, ex: abcnews.com -> 5
    source_cat_dict = dict(zip(sources_list.source,sources_list.status.apply(lambda x:label_codes[x])))
    #mapping the sources to their id (row number), ex: abcnews.com -> 2
    source_id_dict = dict(zip(sources_list.source,sources_list.index))
    return source_cat_dict, source_id_dict

def find_all_refs(wikitext):
    '''
    Method using regex to find all the text between <ref> and <\ref> 
    '''
    return re.findall('\<ref[^\/]*?>(.*?)\<\/ref\>?', wikitext,flags=re.DOTALL)

def find_url_inref(url, ref):
    '''
    Method to check if url exists in the text
    '''
    return re.findall('[./=]{0}'.format(url.replace('.','\.')),ref)

def collect_existing_perennials(source_cat_dict, source_id_dict, snapshot):
    schema = T.ArrayType(T.StructType([
        T.StructField("source_id", T.IntegerType(), False),
        T.StructField("category", T.IntegerType(), False)
    ]))

    @F.udf(returnType=schema)
    def getPerennialCategory(wikitext):
        """
        Extract perennial source ids (if exist) along with the category from article raw text    
        wikitext: raw article text
        """
        output = [] 
        try:
            for ref in find_all_refs(wikitext): 
                for url,cat in source_cat_dict.items(): 
                    if url.lower() in ref.lower():  #quick check, this is 10 times faster than the next
                        if find_url_inref(url.lower(), ref.lower()):                        
                            output.append([source_id_dict[url],cat])
            return output
        except Exception:
            return None 

    wikitext = spark.sql('''SELECT wiki_db, page_id, revision_id, revision_text
                                FROM wmf.mediawiki_wikitext_current
                                WHERE snapshot ="{snapshot}" AND page_namespace = 0 '''.format(snapshot=snapshot))

    df = wikitext.withColumn('perennial', getPerennialCategory(F.col('revision_text'))).drop("revision_text")

    return df
    
source_cat_dict, source_id_dict = source_mappings(sources_list_path)
collect_existing_perennials(source_cat_dict, source_id_dict, snapshot).write.parquet(SAVE_PATH+'wikicurrent_perennials.parquet', mode='overwrite')