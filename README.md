This repository presents the code used in our paper referenced below. Please cite our work if using any data or script:

Aitolkyn Baigutanova, Diego Saez-Trumper, Miriam Redi, Meeyoung Cha, and Pablo Aragón. 2023. A Comparative Study of Reference Reliability in Multiple Language Editions of Wikipedia. In Proceedings of the 32nd ACM International Conference on Information and Knowledge Management (CIKM '23). 

Link to the paper: https://dl.acm.org/doi/10.1145/3583780.3615254

## Data Collection

### 1. Extract Perennial SOurces List from Wikipedia 
The script is written for English perennial sources list. If you want to use other languages, modifications may be required. 

```commandline
python perennial_list.py
```

Output: creates a csv file in data/output directory, which is English perennial sources list


### 2. Collect latest versions of pages that contain links to perennial sources 
The script uses mediawiki_wikitext_current dump to obtain the pages that reference perennial sources across all wiki_dbs
 
Output: creates a parquet file in data/output directory, which contains wiki_db, page_id, revision_id, perennial


### 3. Collect item_ids for the pages and combine with the output from Step #2
The sript uses wikidata_item_page_link dump to obtain item_id of pages along with wiki_db, page_id, page_title
Further these data is joined to the output from step #2.

Output: dataframe with the following columns: wiki_db, page_id, revision_id, item_id, page_title, source_id, category