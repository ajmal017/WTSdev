import shutil, os
import pathlib
import zipfile

import requests
from pathlib import Path

from datetime import datetime, timedelta
import time
import psycopg2
import wtsdblib
import click

# Copies the file into this folder.
download_path = '/home/wts/dev/nse'
backup_path = '/home/wts/dev/nsebkp'

def download(download_url, file_path, backup_file_path):
    """
    download function is used to fetch the data
    """
    print("Downloading file at", file_path)
    # Don't download file if we've done that already
    if os.path.exists(file_path[0:-4]) or os.path.exists(backup_file_path[0:-4]):
        print("We already have this file cached locally or in backup")
        return 1
    else:
        file_to_save = open(file_path, "wb")
        #with requests.get(download_url, verify=True, stream=True) as response:
        try:
            response = requests.get(download_url, verify=True, stream=True,timeout=2)
            if response.status_code != 200:
                print('HTTP response - Not successful')
                return 1
            for chunk in response.iter_content(chunk_size=1024):
                file_to_save.write(chunk)
            file_to_save.close()
            response.close()
            print("Completed downloading file")
        except requests.exceptions.ConnectionError as err:
            print('Connection error: {}'.format(err))
            return 1
        except requests.exceptions.Timeout as err:
            print('File not found in remote server: {}'.format(err))
            return 1



def download_and_unzip(download_url, file_path, backup_file_path):
    """
    download_and_unzip takes care of both downloading and uncompressing
    """
    download(download_url, file_path, backup_file_path)
    with zipfile.ZipFile(file_path, "r") as compressed_file:
        compressed_file.extractall(Path(file_path).parent)
    print("Completed un-compressing")

def download_nse_bhavcopy(for_date):
    """
    this function is used to download bhavcopy from NSE
    """
    for_date_parsed = datetime.strptime(for_date, "%d/%m/%Y")
    month = for_date_parsed.strftime("%b").upper()
    year = for_date_parsed.year
    day = "%02d" % for_date_parsed.day
    url = f"https://archives.nseindia.com/content/historical/EQUITIES/{year}/{month}/cm{day}{month}{year}bhav.csv.zip"
    file_path = os.path.join(download_path, f"cm{day}{month}{year}bhav.csv.zip")
    backup_file_path = os.path.join(backup_path, f"cm{day}{month}{year}bhav.csv.zip")

    try:
        download_and_unzip(url, file_path, backup_file_path)
    except zipfile.BadZipFile:
        print(f"Skipping downloading data for {for_date}")
        return 1
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)
        return 0

def main():

    # Step 1: Download NSE EOD (bhavcopy) files from start_date to end_date

    skip_downloading = False
    if (skip_downloading):
        print('Skipping file download from NSE - as per flag setting')
    else:
        end_date = datetime.now()
        delta = timedelta(days=30)
        start_date = end_date - delta
        # Do not edit above three lines... For temporary runs, override start_date and end_date by uncommenting and editing below two lines
        #start_date = datetime(year=2020, month=1, day=1)
        #end_date = datetime(year=2021, month=3, day=31)

        process_date = start_date
        while process_date <= end_date:
            str_process_date = datetime.strftime(process_date, '%d/%m/%Y')
            download_nse_bhavcopy(str_process_date)
            delta = timedelta(days=1)
            process_date = process_date + delta

    # Step 2: Import all the CSV files from download_directory into NSE_EOD_DATA table and move them to backup_directory.

    print (f"Uploading files from {download_path} into the database")
    dbconn = wtsdblib.wtsdbconn.newconnection('WTSDEV')
    for filename in os.listdir(download_path):
        try:
            dbcursor = dbconn.cursor()
            f = open( os.path.join(download_path,filename),'r')
            f.readline() # Readline makes the file pointer f to skip the header (first line)
            dbcursor.copy_from(f,'wtst.nse_eod_data(symbol, series, open, high, low, close, last, prevclose, volume, tradevalue, date, tradecount, isincode, dummy)', sep=',', null='\\N', size=8192, columns=None)
            f.close()
            dbconn.commit()
            print(f"Successfully uploaded data from {filename}")
            shutil.move(os.path.join(download_path, filename), os.path.join(backup_path, filename))
            dbcursor.close()
            dbconn.commit()
        except psycopg2.Error as err:
            print(f"Db error: {filename} : {err}")
            # Continue execution with new DB connection.
            dbconn.close()
            dbconn = wtsdblib.wtsdbconn.newconnection('WTSDEV')
            continue
        except (Exception, psycopg2.Error) as err:
            print('Non-database error in main - Step 2')
            dbcursor.close()

    dbconn.commit()
    dbconn.close()

if __name__ == '__main__':
    main()