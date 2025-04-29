# XML Parsing Pipeline
This project was created to quickly extract data from open access scientific articles. The extracted data can be used to train models or conduct analyses. 
- functions.py: contains the code for all functions called on in the parsing pipeline.
- parser_pipeline.py: contains the pipeline, and can be run on your own files by changing the download_path that is hard coded into this script.

## File Organization
The PMC open access archive that this pipeline was developed for can be found at https://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_package/. This pipeline will process one subfolder from the oa_package archive from start to finish before moving on to the next subfolder. This was done to facilitate running this pipeline in an HPC environment where large downloads are not allowed. For example, all the publications within oa_package/00/00 will be downloaded, parsed, and output files will be generated before moving on to oa_package/00/01.

## Running parser_pipeline.py
The parser_pipeline has been tested within a conda viritual environment and requires the following software to be installed: 
- Python 3.12.9 
- conda 25.3.1 (not required if not running in a virtual environment)
- beautifulsoup4 4.12.3
- pandas 2.2.3

Specify your download_path in the parser_pipeline.py file. This is the folder where all papers will be dowloaded, and where you will find the final output files. One set of output files will be created for every PMC subfolder (i.e., oa_package/00/00).
