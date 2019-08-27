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

    def download_bhavcopy(self):
        arr = []
        response = self.nse.download_bhavcopy(str(datetime.now() - timedelta(days=1)))
        s = io.StringIO(response.decode('utf-8'))
        headers = s.readlines(1)[0].split(',')
        #pprint(headers)
        for line in s:
            fields = map(lambda s: s.strip(), line.split(','))
            columns = list(fields)
            for i in range(len(headers)):
                arr.append({
                    headers[i]:columns[i]
                })
        pd.read_json(json.dumps(arr)).to_excel("filename.xlsx")

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

    # while True:
    #     now = datetime.now()
    #     if(end_time <= now - timedelta(minutes=3)):
    #         #this is bad way for time management, but for now, get the data every 3 min
    #         #later this needs to be set as cron job
    #         print("I need to take the spreadsheet now")
    #         end_time = now
    #         nseDataFetch.get_equity_stock_watch_live()
    #
    #     time.sleep(120)
    #     if end_time > today5pm:
    #         break

        #processing for the day is done, now download the bhav copy
    nseDataFetch.download_bhavcopy()


if __name__ == "__main__":
    main()