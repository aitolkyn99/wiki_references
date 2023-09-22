# This code is accessing a WMF data lakes table

import pyspark.sql.functions as F
import pyspark.sql.types as T
import re 


SAVE_PATH = 'data/output/'

def extract_perennials(cat_dict, source_id_dict, snapshot):
    schema = T.ArrayType(T.StructType([
        T.StructField("source_id", T.IntegerType(), False),
        T.StructField("category", T.IntegerType(), False)
    ]))

    cat_dict = {} # define cat_dict
    source_id_dict = {} 

    @F.udf(returnType=schema)
    def getPerennialCategory(wikitext):
        """
        Extract perennial source ids (if exist) along with the category from article raw text    
        wikitext: raw article text
        cat_dict: dictionay mapping sources to category class (1: deprecated, 2:deprecated, 3:gen.unreliable, 4:no consensus, 5:gen.reliable
        source_id_dict: dictionay mapping sources to source id 
        """
        output = [] 
        try:
            #regex to find all the text between <ref> and <\ref>
            for ref in re.findall('\<ref[^\/]*?>(.*?)\<\/ref\>?', wikitext,flags=re.DOTALL): 
                for url,cat in cat_dict.items(): 
                    if url.lower() in ref.lower():  #quick check, this is 10 times faster than the next
                        if re.findall('[./=]{0}'.format(url.lower().replace('.','\.')),ref.lower()):                        
                            output.append([source_id_dict[url],cat])
            return output
        except Exception:
            return None 


    #set snapshot 
    wikitext = spark.sql('''SELECT wiki_db,page_id,revision_id,revision_text
                                FROM wmf.mediawiki_wikitext_current
                                WHERE snapshot ="{snapshot}" AND page_namespace = 0 '''.format(snapshot=snapshot))

    df = wikitext.withColumn('perennial', getPerennialCategory(F.col('revision_text'))).drop("revision_text")

    return df
    

extract_perennials(cat_dict,source_id_dict,snapshot).write.parquet(SAVE_PATH+'wikicurrent_perennials.parquet', mode='overwrite')