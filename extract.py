import zipfile
import os
import csv
from datetime import datetime
from bs4 import BeautifulSoup
import requests
import shutil
import pandas as pd

import time
 

# download missing data from french customs website
#
# data files contain all monthly imports/exports by products (all detailed NC8 ~10325, CPF6 and A129 categories) and countries since 2004

debug = 0

cur_date = datetime.now()
cur_year = cur_date.year
print(cur_date.date())

# set dir name for downloads storage starting from ./
download_dir = "downloads"

# gather Douane data
def download_file(url):
    local_filename = "%s/%s"% (download_dir, url.split('/')[-1])
    with requests.get(url, stream=True) as r:
        with open(local_filename, 'wb') as f:
            shutil.copyfileobj(r.raw, f)

    return local_filename

#get downloads
douane_url="https://lekiosque.finances.gouv.fr/site_fr/telechargement/telechargement_SGBD.asp"
r  = requests.get(douane_url)
data = r.text
soup = BeautifulSoup(data,features="lxml")

# filter for national Import/export data files
my_href = []
print("files to download")
for link in soup.find_all('a'):
    href = link.get('href')
    if ("zip" and "national") in href.lower():
        print(href)
        my_href.append(href)

file_list = []
#get  files
try:
    existing_files = os.listdir(download_dir)
except FileNotFoundError:
    # create dir
    os.makedirs(download_dir)
    existing_files = os.listdir(download_dir)

for f in my_href:
# for f in my_href[:4]:
    if f.split("/")[-1] in existing_files:
        #skip existing files
        print("skip %s"%f.split("/")[-1])
        continue
    print("Downloading %s"%f.split("/")[-1])
    if not debug:
        file = download_file(f)

# unzip if needed
for x in os.listdir(download_dir):
    if x.endswith(".zip"):
        myfile = "%s/%s"% (download_dir, x)
        # print(myfile)
        with zipfile.ZipFile(myfile,"r") as zip_ref:
            print(zip_ref.namelist()[0])
            try:
                # check if unzipped file exists
                tmp = os.listdir("%s/%s"% (download_dir, zip_ref.namelist()[0]))
                print("skip unzipping %s"%myfile)
            except FileNotFoundError:
                # extract (creates directories)
                print("extract %s"%myfile)
                zip_ref.extractall("./%s" % download_dir)

# list of extracted directories
data_dirs = ["%s/%s"%(download_dir,x) for x in os.listdir(download_dir) if "zip" not in x ]
print(data_dirs)

dbase = 1
if dbase:
    # custom detailed categories
    # load table for the 17 categories which will be created from the NC8 categories
    mycategories = pd.read_csv('correspondance_categories.csv', header=None, index_col=0, squeeze=True, sep=";", dtype=str).to_dict()
    # print(mycategories)

    # custom synthetic categories
    # load table for the 8 categories which will be created from the previous 17 categories
    reduced_categories = pd.read_csv('reduced_categories.csv', header=None, index_col=0, squeeze=True, sep=";", dtype=str).to_dict()
    # print(reduced_categories)

    col_names = ["flux","month","year","cat","country","amount"]
    csv_files = []
    years = []
    if not debug:
        # create csv file list for dataframe creation
        for x in data_dirs:
        # for x in data_dirs[:3]:
            nname = x.split("-")
            # flux : import or export from dir name
            flux = nname[-1]
            try:
                # construct csv file name
                year = int(nname[1])
                years.append(year)
                ffile = x + "\\" + "_".join(["National",str(year),"%s%s.txt"%(flux[0].upper(), flux[1:])])
            except ValueError:
                # if error -> no year in filename, its the last data file containing the 12 most recent months
                # keep only the last year data
                # current file name is : NATIONAL_NC8PAYS[E/I].txt
                ffile = x + "\\NATIONAL_NC8PAYS%s.txt"%flux[0].upper()
                with open(ffile) as tmpfile:
                    try:
                        # year = int(tmpfile.readline().split(";")[2])
                        year = tmpfile.readline().split(";")[2]
                    except:
                        # skip file
                        continue
                print ("extract last year data from file %s and create new data file" % ffile)
                # keep only last year data
                tmp = (pd.read_csv(ffile, header=None, sep=";", dtype=str) [lambda x: x[2]==year])
                # write new data file with correct filename structure : National_year_Export/Import.txt
                new_name = x + "\\" + "_".join( [ "National", year, "%s%s.txt" % (flux[0].upper(), flux[1:]) ] )
                tmp.to_csv(new_name, header=False, sep=';', index=False)
                ffile = new_name
            except IndexError:
                continue
            print (" adding %s file" % ffile)
            csv_files.append(ffile)
            # data = pd.read_csv(ffile, header=None, usecols=[0,1,2,5,6,7], sep=";")   
            # df = pd.DataFrame(data)
            # df.columns=col_names
            # print(df)
        # create dataframe from all csv files

        print("Concat csv files in dataframe...")

        t0 = time.time()
        data = pd.concat(
                        # [ pd.read_csv( f, header=None, usecols=[0,1,2,5,6,7], sep=";" )  for f in csv_files ],
                        [ pd.read_csv( f, header=None, usecols=[0,1,2,5,6,7], sep=";", dtype=str )  for f in csv_files ],
                        ignore_index=True )
        data.columns = col_names
        print(" elapsed time %s s" % (time.time()-t0))
        print(data.head())
        print(data.dtypes)
        nb_rows = data.shape[0]
        print(" number of rows %s" % nb_rows)
    else:
        # debug
        data = pd.read_csv("tmp.csv", sep=';')
        # data.to_csv("tmp.csv", sep=';', index=False)

    if data.dtypes[5] == "object":
        t0 = time.time()
        # convert amount to float
        print("Convert 'amount' to float...")

        # fill empty cells with 0
        data["amount"] = pd.to_numeric(data["amount"], errors="coerce").fillna(0).astype(int)
        print(" elapsed time %s s" % (time.time()-t0))

    # sum the months for years per cat, countries and flux
    print("sum the months...")

    t0 = time.time()
    data = data.groupby(["flux", "year", "cat", "country"])["amount"].sum().reset_index()
    print(" elapsed time %s s" % (time.time()-t0))

    # keep lines where length of NC8 cat is >= 2  
    # data = data[data['cat'].apply(lambda x: len(x) > 1)]

    # create main categories (first2 digits of cat-NC8) and sum
    # data["main_cat"] = data['cat'].astype(str).str[:2].astype(int)
    print("regroup by 2 first digits categories...")

    t0 = time.time()
    print("reduction to first 2 digits categories...")
    if data.dtypes[3] == "object":
        data["main_cat"] = data['cat'].str[:2]
    elif data.dtypes[3] == "int64":
        data["cat"] = data['cat']/1000000
        data["main_cat"] = data['cat'].astype(int)

    print(" elapsed time %s s" % (time.time()-t0))

    print("sum of categories...")
    t0 = time.time()
    data = data.groupby(["flux", "year", "main_cat", "country"])["amount"].sum().reset_index()
    print(" elapsed time %s s" % (time.time()-t0))

    # map the 17 categories from dict mycategories and sum
    print("regroup by custom categories...")
    t0 = time.time()

    # data["my_cat"] = data["main_cat"].astype(str).map(mycategories)
    data["my_cat"] = data["main_cat"].map(mycategories)
    print(" elapsed time %s s" % (time.time()-t0))

    t0 = time.time()
    data = data.groupby(["flux", "year", "my_cat", "country"])["amount"].sum().reset_index()
    print(" elapsed time %s s" % (time.time()-t0))

    # map the 8 categories from dict reduced_categories and sum
    print("regroup by custom main categories...")

    t0 = time.time()
    # data["red_cat"] = data["my_cat"].astype(str).map(reduced_categories)
    data["red_cat"] = data["my_cat"].map(reduced_categories)
    print(" elapsed time %s s" % (time.time()-t0))

    t0 = time.time()
    data = data.groupby(["flux", "year", "red_cat", "country"])["amount"].sum().reset_index()
    print(" elapsed time %s s" % (time.time()-t0))

    # In case needed dump csv with all countries all years exports and imports 
    print("dump file custom main categories with all countries and years...")

    t0 = time.time()
    data.to_csv( "imports_exports_by_reduced_categories_and_countries.csv", sep=';', encoding='latin1')
    print(" elapsed time %s s" % (time.time()-t0))

    # sum over countries
    print("Sum over countries for world...")

    t0 = time.time()
    data = data.groupby(["flux", "year", "red_cat"])["amount"].sum().reset_index()
    print(" elapsed time %s s" % (time.time()-t0))

    print("dump export and import files custom main categories for world...")
    # exports
    data[data["flux"] == "E"].pivot_table(
        values="amount", index="year", columns="red_cat",
        aggfunc="sum").to_csv(
        "FR_exports_by_reduced_categories.csv", sep=';', encoding='latin1')
    # imports
    data[data["flux"] == "I"].pivot_table(
        values="amount", index="year", columns="red_cat",
        aggfunc="sum").to_csv(
        "FR_imports_by_reduced_categories.csv", sep=';', encoding='latin1')

    # pivot for categories
    # data = data.pivot_table(
    #     values="amount", index=["flux", "year"], columns="red_cat",
    #     aggfunc="sum")
    # data.to_csv("tmp2.csv", sep=';', index=False)
quit()
