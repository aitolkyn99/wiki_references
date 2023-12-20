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
snapshot_items = '2023-05-01'
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
    '''
    Method to collect pages referencing perennial sources from mediawiki_wikitext_current dump
    '''
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

def collect_item_ids(snapshot_items):
    '''
    Method to collect item_ids from wikidata_item_page_link dump
    '''
    
    wikidata = spark.sql('''
                    SELECT sq.item_id, 
                        collect_list(sq.wiki_db ) as wiki_dbs,
                        collect_list(sq.page_id ) as page_ids,
                        collect_list(sq.page_title ) as page_titles
                    FROM 
                        ( SELECT * 
                            FROM wmf.wikidata_item_page_link
                            WHERE snapshot="{snapshot}" AND
                                page_namespace=0 AND
                                wiki_db LIKE '%wiki'
                            SORT BY wiki_db
                        ) sq    
                    GROUP BY sq.item_id
                    '''.format(snapshot=snapshot_items))

    df = wikidata.withColumn('wiki_db_page_id', F.explode(F.arrays_zip("wiki_dbs","page_ids","page_titles"))).selectExpr('item_id','wiki_db_page_id.wiki_dbs as wiki_db','wiki_db_page_id.page_ids as page_id','wiki_db_page_id.page_titles as page_title')
    return df


def join_perennials_items():
    source_cat_dict, source_id_dict = source_mappings(sources_list_path)
    df1 = collect_existing_perennials(source_cat_dict, source_id_dict, snapshot)
    df2 = collect_item_ids(snapshot_items).distinct()

    col = 'perennial'
    df = df1.join(df2, ['wiki_db','page_id']).select('wiki_db','page_id','revision_id','item_id','page_title',F.explode(col).alias('perennial')).\
    select('wiki_db','page_id','revision_id','item_id','perennial.source_id','perennial.category').\
    groupby('wiki_db','page_id','revision_id','item_id','source_id','category').count()

    df.write.parquet(SAVE_PATH+'final_data_enperennials.parquet', mode='overwrite')

join_perennials_items()

