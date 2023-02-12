# extract_French_customs
Extract Import/export data per product per country since 2004

2004-2022 ~4G of unzipped files

copy all 3 files in working dir

csv files contain custom categories definitions based on NC8 nomenclature

Script extract.py Python 3.6

imports :
from bs4 import BeautifulSoup
import requests
import shutil
import pandas as pd
