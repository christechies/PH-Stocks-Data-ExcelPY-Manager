import json
import time
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.firefox.options import Options

class mw_dataHandler:
    def __init__(self):
        self.mw_list = 'https://www.marketwatch.com/tools/markets/stocks/country/philippines'
        self.mw_mainlink = 'https://www.marketwatch.com'
        self.page_count = 2
    def setSelenium(self):
        options=Options()
        options.add_argument('start-maximized')
        options.add_argument('--headless')
        driver=webdriver.Firefox(options=options,executable_path=r'Y:\geckodriver.exe')
        return driver
    def mw_setStockData(self,mw_link,main_link,pages=self.page_count,driver=None):
        page_data = []
        metadata = []
        for i in range(1,pages+1):
            cur_link = mw_link+'/{num}'.format(num=i)
            myDriver = driver if driver != None else self.setSelenium()
            myDriver.get(mw_link)
            mw_page = BeautifulSoup(myDriver.page_source,'html.parser')
            page_data.append(mw_page)
            myDriver.quit()
        for mw_page in page_data:    
            for i in mw_page.find_all('tr'):
                scrape_ins = i.find_all('td')
                if scrape_ins ==[]:
                    continue
                data_ins = {
                    'name':scrape_ins[0].find('a').text,
                    'symbol':scrape_ins[0].find('small').text[1:-1],
                    'link':main_link+scrape_ins[0].find('a')['href'],
                    'f_exchange':scrape_ins[1].text,
                    'sector':scrape_ins[2].text
                }
                metadata.append(data_ins)
        return metadata
    def extract_overview(self,stock_link,driver=None):
        overview_data=[]
        myDriver = driver if driver != None else self.setSelenium()
        myDriver.get(stock_link)
        page = BeautifulSoup(myDriver.page_source,'html.parser')
        myDriver.quit()
        over_list = page.find('ul',attrs={'class':'list list--kv list--col50'})
        data_current = {}
        for i in over_list.find_all('li'):
            try:
                title = i.find('small',attrs={'class':'label'}).text
                data = i.find('span',attrs={'class':'primary'}).text
            except AttributeError:
                continue
            data_current[title.replace(' ','_')] = data
        return data_current
    def extract_finProf(self,stock_link,driver=None):
        myDriver = driver if driver != None else self.setSelenium()
        myDriver.get(stock_link)
        page = BeautifulSoup(myDriver.page_source,'html.parser')
        myDriver.quit()
        title_holder = ''
        finProf_data = []
        fin_data ={}
        for i in page.find_all('div',attrs={'class':'element element--table'}):
            header=i.find('header',attrs={'class':'header header--secondary'})
            head = header.find('span',attrs={'class':'label'}).text.lower()[:3]
            table = i.find('table')
            for i,x in enumerate(table.find_all('td')):
                if (i+1)%2==0:
                    fin_data[head+'_'+title_holder.replace(' ','_')] = x.text
                title_holder=x.text
        return fin_data
    def extract_histQuotes(self,stock_link,driver=None):
        myDriver = driver if driver != None else self.setSelenium()
        myDriver.get(stock_link)
        page = BeautifulSoup(myDriver.page_source,'html.parser')
        myDriver.quit()
        histQuotes = []
        table = page.find('table',attrs={'class':'table table--overflow align--center'})
        for i in table.find_all('tr')[1:]:
            try:
                date = i.find_all('td')[0].find('div').text
                open_p = i.find_all('td')[1].find('div').text
                high_p = i.find_all('td')[2].find('div').text
                low_p = i.find_all('td')[3].find('div').text
                close_p = i.find_all('td')[4].find('div').text
                volume = i.find_all('td')[5].find('div').text
            except AttributeError:
                continue
            data_full = {
                'date':date,
                'open_price':open_p,
                'high_price':high_p,
                'low_price':low_p,
                'closed_price':close_p,
                'volume':volume
            }
            histQuotes.append(data_full)
        return histQuotes
if __name__=='__main__':
    app = mw_dataHandler()