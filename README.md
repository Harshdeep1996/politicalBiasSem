## Extension of Citations dataset: using bias and reliability 
Preliminary work around trying to map the citations in the Citations Dataset by extracting the domain names with the following:

1. To get bias score: map it with Media Monitor (https://homepages.dcc.ufmg.br/~lucaslima/pdf/ribeiro2018media.pdf) Dataset
2. To get reliability label: map it with Media Bias Fact Check (https://mediabiasfactcheck.com/) dataset 

Contains the following files and folders:

- *analysis_bias_citations.ipynb/*: Notebook containing work of the analysis on the sources by combining different data sources with citations
- *dataset/*: Contains all the intermediary datasets which were used to do the analysis
- *results/*: Images of the results which were used to draft in the report and which were extracted from the notebook
- *scripts/*: Contains all the scripts which were used to generate the features for the citations, MM and MBFC dataset and map them together. The scripts allow you to generate the *citations_bias_features.parquet* dataset which is the final dataset which can be used to run in the notebook to do the analysis.

More documentation around the various other reliability sources which can be used in the future have been compiled in a Google doc: https://docs.google.com/document/d/16SWYU44owyM4G4pi-RhAG4eiPwcuzBEtLQD_AeF_ing/edit
