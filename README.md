This repository presents the code used in our paper referenced below. Please cite our work if using any data or script:

<em>Aitolkyn Baigutanova, Diego Saez-Trumper, Miriam Redi, Meeyoung Cha, and Pablo Aragón. 2023. A Comparative Study of Reference Reliability in Multiple Language Editions of Wikipedia. In Proceedings of the 32nd ACM International Conference on Information and Knowledge Management (CIKM '23).</em> 

Link to the paper: https://dl.acm.org/doi/10.1145/3583780.3615254

## Data Collection

### 1. Extract Perennial Sources List from Wikipedia 
The script is written for English perennial sources list. If you want to use other languages, modifications may be required. 

```commandline
python perennial_list.py
```

Output: creates a csv file in data/output directory, which is English perennial sources list


### 2. Collect latest versions of pages that contain links to perennial sources 
The script uses mediawiki_wikitext_current dump to obtain the pages that reference perennial sources across all wiki_dbs.
Function <em>collect_existing_perennials</em> in data_collection.py

Output: dataframe with the following columns: wiki_db, page_id, revision_id, perennial


### 3. Collect item_ids for the pages and combine with the output from Step #2
The sript uses wikidata_item_page_link dump to obtain item_id of pages. 
Function <em>collect_item_ids</em> in data_collection.py

Output: dataframe with the following columns: wiki_db, page_id, item_id, page_title


### 4. Join Data 
Join data from steps #2 and #3 

```commandline
python data_collection.py
```

Output: creates a parquet file in data/output directory, which contains dataframe with the following columns: wiki_db, page_id, revision_id, item_id, page_title, source_id, category


## Citation
**A Comparative Study of Reference Reliability in Multiple Language Editions of Wikipedia**
```
@inproceedings{10.1145/3583780.3615254,
author = {Baigutanova, Aitolkyn and Saez-Trumper, Diego and Redi, Miriam and Cha, Meeyoung and Arag\'{o}n, Pablo},
title = {A Comparative Study of Reference Reliability in Multiple Language Editions of Wikipedia},
year = {2023},
isbn = {9798400701245},
publisher = {Association for Computing Machinery},
address = {New York, NY, USA},
url = {https://doi.org/10.1145/3583780.3615254},
doi = {10.1145/3583780.3615254},
abstract = {Information presented in Wikipedia articles must be attributable to reliable published sources in the form of references. This study examines over 5 million Wikipedia articles to assess the reliability of references in multiple language editions. We quantify the cross-lingual patterns of the perennial sources list, a collection of reliability labels for web domains identified and collaboratively agreed upon by Wikipedia editors. We discover that some sources (or web domains) deemed untrustworthy in one language (i.e., English) continue to appear in articles in other languages. This trend is especially evident with sources tailored for smaller communities. Furthermore, non-authoritative sources found in the English version of a page tend to persist in other language versions of that page. We finally present a case study on the Chinese, Russian, and Swedish Wikipedias to demonstrate a discrepancy in reference reliability across cultures. Our finding highlights future challenges in coordinating global knowledge on source reliability.},
booktitle = {Proceedings of the 32nd ACM International Conference on Information and Knowledge Management},
pages = {3743–3747},
numpages = {5},
keywords = {misinformation, information credibility, verifiability, fake news, wikipedia},
location = {<conf-loc>, <city>Birmingham</city>, <country>United Kingdom</country>, </conf-loc>},
series = {CIKM '23}
}
```
