import os
import time
import random
import re
import pandas as pd
import requests
from lxml import etree
import datetime
from dateutil.relativedelta import relativedelta
from zhconv import convert # 中文繁简体转换
from retrying import retry
requests.packages.urllib3.disable_warnings()


# ===================设置路径=======================
hk_data_download_path = 'download_data'  # 下载原始数据
if not os.path.exists(hk_data_download_path):
    os.makedirs(hk_data_download_path)

# 日期设置：默认起始日期为半年前，结束日期为昨天，剔除已经下载过的数据日期
files = os.listdir(hk_data_download_path)
downloaded_dates = list(map(lambda x: x[:8], files))

start_date = datetime.datetime.today() - relativedelta(months=6)
weekday = [1, 2, 3, 4, 5]
while start_date.isoweekday() not in weekday:
    start_date -= relativedelta(days=1)
start_date = start_date.strftime('%Y%m%d')

lastday = datetime.datetime.today() - relativedelta(days=1)
weekday = [1, 2, 3, 4, 5]
while lastday.isoweekday() not in weekday:
    lastday -= relativedelta(days=1)
lastweekday = lastday.strftime('%Y%m%d')
end_date = lastweekday

start_date = end_date

date_range = list(map(lambda x: x.strftime('%Y%m%d'), pd.bdate_range(start_date, end_date).tolist()))
dates = [date for date in date_range if date not in downloaded_dates]

date_info = {'begin_date': dates[0], 'end_date': dates[-1]}

# ===============爬虫参数设置=======================
user_agent_list = [
    "Mozilla/5.0 (Windows NT 10.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0) Gecko/20100101 Firefox/61.0",
    "Mozilla/5.0 (Windows NT 10.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.186 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.101 Safari/537.36",
    "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.112 Safari/537.36"
    ]

header = {"User-Agent":
              "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.112 Safari/537.36"}

# url = "https://www.hkexnews.hk/sdw/search/searchsdw_c.aspx"
# ticker_url = "https://www.hkexnews.hk/sdw/search/stocklist_c.aspx?sortby=stockcode&shareholdingdate="

url = 'https://www3.hkexnews.hk/sdw/search/searchsdw_c.aspx'
ticker_url = 'https://www3.hkexnews.hk/sdw/search/stocklist_c.aspx?sortby=stockcode&shareholdingdate='

para = {'__EVENTTARGET': 'btnSearch',
        '__EVENTARGUMENT': '',
        '__VIEWSTATE': '/wEPDwUKMTY0ODYwNTA0OWRkM79k2SfZ+VkDy88JRhbk+XZIdM0=',
        '__VIEWSTATEGENERATOR': '3B50BBBD',
        'today': datetime.date.today().strftime('%Y%m%d'),
        'sortBy': 'shareholding',
        'sortDirection': 'desc',
        'alertMsg': '',
        'txtShareholdingDate': '',  # %Y/%m/%d
        'txtStockCode': '',
        'txtStockName': '',
        'txtParticipantID': '',
        'txtParticipantName': ''
        }


class BuildSpider:

    def __init__(self, header, url, ticker_url, para, date_info):
        self.header = header
        self.url = url
        self.tickers_url = ticker_url
        self.para = para
        self.date_info = date_info
        self.dates = self.datelist()
        self.tickers = None  # wind code list
        self.tickers_dict = None  # key: wind code, value: hk tickers
        self.names_dict = None  # key: wind code, value: stock name

    @retry(stop_max_attempt_number=5)
    def get_tickers(self, date):
        '''
        获取每日最新的港交所交易股票代码
        '''
        print('updating tickers on {}'.format(date), end=' ')
        ticker_url = '{0}{1}'.format(self.tickers_url, date)
        # para = {'sortby': 'stockcode',
        #         'shareholdingdate': date}
        s = requests.session()
        s.keep_alive = False
        # print(ticker_url)
        # response = requests.post(ticker_url, headers=self.header, data=para, timeout=10)
        response = requests.get(ticker_url)
        text = response.text
        pairs = re.findall(r'[{](.*?)[}]', text)

        hk_tickers = []
        hk_names = []

        for pair in pairs:
            pair = re.findall(r'["](.*?)["]', pair)
            hk_ticker = pair[1]
            hk_name = pair[-1]
            hk_tickers.append(hk_ticker)
            hk_names.append(hk_name)

        # html = etree.HTML(text)
        # table = html.xpath("//table[@class='table']/tbody/tr")
        # hk_tickers = []
        # hk_names = []
        # for tr in table:
        #     # hk_ticker = tr.xpath("./td[@style='text-align: center;']/text()")[0].strip()
        #     # hk_name = tr.xpath("./td[@style='text-align: left;']/a/text()")[0].strip()
        #     if hk_ticker[0] in ['7', '9']:  # 港交所股票以0开头，深交所股票以7开头，上交所以9开头
        #         hk_tickers.append(hk_ticker)
        #         hk_names.append(hk_name)
        tickers_df = pd.DataFrame(data={'hk_ticker': hk_tickers, 'hk_name': hk_names})
        print('get')

        def hk_to_wind(hk_ticker):
            if hk_ticker[0] == '9':
                return '60{}.SH'.format(hk_ticker[1:])
            else:
                if hk_ticker[1] == '7':
                    return '300{}.SZ'.format(hk_ticker[2:])
                else:
                    return '00{}.SZ'.format(hk_ticker[1:])

        def convert_ch(name):
            return convert(name, 'zh-cn')

        tickers_df['ticker'] = tickers_df['hk_ticker'].map(hk_to_wind)
        tickers_df['name'] = tickers_df['hk_name'].map(convert_ch)

        self.tickers = tickers_df['ticker'].tolist()
        self.tickers_dict = tickers_df.set_index('ticker')['hk_ticker'].to_dict()
        self.names_dict = tickers_df.set_index('ticker')['name'].to_dict()

    @retry(stop_max_attempt_number=10)
    def get_table(self, date, ticker):
        print('date:{0}, ticker:{1}'.format(date, ticker))
        input_date = datetime.datetime.strptime(date, '%Y%m%d').strftime('%Y/%m/%d')  # 修改日期格式
        self.para['txtShareholdingDate'] = input_date
        self.para['txtStockCode'] = self.tickers_dict[ticker]
        s = requests.session()
        s.keep_alive = False
        response = requests.post(self.url, headers=self.header, data=self.para, verify=False)
        text = response.text
        html = etree.HTML(text)
        div = html.xpath("//div[@id='pnlResultNormal']//tbody/tr")
        df_info = pd.DataFrame(columns=['date', 'ticker', 'participant_id', 'participant_name', 'shareholding',
                                        'shareholding_percent'])
        df_total = pd.DataFrame(columns=['wind_code', 'shareholding', 'participants', 'percent', 'total_share'])
        for tb in div:
            try:
                if len(tb.xpath("./td[@class='col-participant-id']/div/text()")) == 2:
                    participant_id = tb.xpath("./td[@class='col-participant-id']/div/text()")[1]
                else:
                    participant_id = 'HK'
                participant_name = tb.xpath("./td[@class='col-participant-name']/div/text()")[1]
                shareholding = tb.xpath("./td[@class='col-shareholding text-right']/div/text()")[1]
                shareholding_percent = tb.xpath("./td[@class='col-shareholding-percent text-right']/"
                                                "div/text()")[1]

                df_info = df_info.append([{'date': input_date, 'ticker': ticker, 'participant_id': participant_id,
                                           'participant_name': participant_name, 'shareholding': shareholding,
                                           'shareholding_percent': shareholding_percent}], ignore_index=True)

            except IndexError:
                print('ticker {0} in {1} not found'.format(ticker, date))
                continue

        try:
            div = html.xpath("//div[@id='pnlResultSummary']/div/div")
            shareholding = div[2].xpath("./div[@class='shareholding']/div/text()")[1]
            participants = div[2].xpath("./div[@class='number-of-participants']/div/text()")[1]
            percent = div[2].xpath("./div[@class='percent-of-participants']/div/text()")[1]
            total_share = div[3].xpath("./div[@class='summary-value']/text()")[0]
            df_total = pd.DataFrame(data=[[ticker, shareholding, participants, percent, total_share]],
                                    columns=['wind_code', 'shareholding', 'participants', 'percent', 'total_share'])
        except IndexError:
            pass

        return df_info, df_total

    def datelist(self):
        print('start_date:{0}, end_date:{1}'.format(self.date_info['begin_date'], self.date_info['end_date']))
        if self.date_info['begin_date'] <= self.date_info['end_date']:
            dates = [datetime.datetime.strftime(x, '%Y%m%d')
                     for x in list(pd.bdate_range(start=self.date_info['begin_date'], end=self.date_info['end_date']))]
            return dates
        else:
            return []

    def run(self):
        '''
        遍历日期和股票代码，合并后返回整个csv
        :return: dataframe
        '''

        for date in self.dates:
            df_info = []
            df_total = []
            self.get_tickers(date)

            for ticker in self.tickers:
                info, total = self.get_table(date, ticker)
                if not info.empty:
                    print(info.head())
                    print(total.head())
                    df_info.append(info)
                    df_total.append(total)

            df_info = pd.concat(df_info, axis=0)
            df_total = pd.concat(df_total, axis=0)
            # df_info.to_csv('{0}/{1}.csv'.format(hk_data_download_path, date), index=False, encoding='utf-8-sig')

    def format(self, df_info):

        df_info['shareholding'] = df_info['shareholding'].map(lambda x: x.replace(',', '')).astype('float64')
        df_info['total_share'] = df_info['total_share'].map(lambda x: x.replace(',', '')).astype('float64')
        df_info['shareholding_percent'] = df_info['shareholding']/df_info['total_share'] * 100

        return df_info


if __name__ == '__main__':
    spider = BuildSpider(url=url, ticker_url=ticker_url, header=header, para=para, date_info=date_info)
    spider.run()
