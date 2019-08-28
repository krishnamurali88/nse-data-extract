__author__ = "muralikrishna"

from nsetools import Nse
from pprint import pprint
import json
from datetime import datetime
import pandas as pd
from nsetools import utils
from datetime import timedelta
import time
import io


class NSEDataFetch(Nse):
    def __init__(self):
        self.nse = Nse()
        self.equity_stock_watch_all = "https://www.nse-india.com/live_market/dynaContent/live_watch/stock_watch/niftyStockWatch.json"

    def get_active_monthly(self):
        pprint(self.nse.get_active_monthly())

    def get_quote(self, symbol):
        pprint(self.nse.get_quote(symbol))

    def download_bhavcopy(self, date_string):
        arr = []
        response = self.nse.download_bhavcopy(date_string)
        s = io.StringIO(response.decode('utf-8'))
        headers = s.readlines(1)[0].split(',')
        filename = "BavCopy{}.xlsx".format(datetime.now())
        for line in s:
            fields = map(lambda s: s.strip(), line.split(','))
            columns = list(fields)
            #bad coding here, need to remove the hardcoded values,
            #need to automate this hardcoded value
            arr.append({
                  headers[0]:columns[0],
                  headers[1]:columns[1],
                  headers[2]:columns[2],
                  headers[3]:columns[3],
                  headers[4]:columns[4],
                  headers[5]:columns[5],
                  headers[6]:columns[6],
                  headers[7]:columns[7],
                  headers[8]:columns[8],
                  headers[9]:columns[9],
                  headers[10]:columns[10],
                  headers[11]:columns[11],
                  headers[12]:columns[12],
                  headers[13]:columns[13]
            })
        pd.read_json(json.dumps(arr)).to_excel(filename)

    def get_equity_stock_watch_live(self):
        response = self.nse._get_json_response_from_url(self.equity_stock_watch_all, True)
        filename= "EquityMatketWatch{}.xls".format(datetime.now())
        df = pd.read_json(response)
        df.to_excel(filename)

    def get_stock_codes(self):
        self.nse.get_stock_codes()


def main():
    nseDataFetch = NSEDataFetch()
    end_time = datetime.now()
    today5pm = end_time.replace(hour=17)

    while True:
        now = datetime.now()
        if(end_time <= now - timedelta(minutes=3)):
            #this is bad way for time management, but for now, get the data every 3 min
            #later this needs to be set as cron job
            print("I need to take the spreadsheet now")
            end_time = now
            nseDataFetch.get_equity_stock_watch_live()
    
        time.sleep(120)
        if end_time > today5pm:
            break

    #processing for the day is done, now download the bhav copy
    # date has to be in the yyyy-mm-dd format
    nseDataFetch.download_bhavcopy("2018-08-28")


if __name__ == "__main__":
    main()