import json
import time
import string
import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.firefox.options import Options

class dataHandler:
    def __init__(self):
        self.page_count = 2
        
    def setSelenium(self):
        options=Options()
        options.add_argument('start-maximized')
        options.add_argument('--headless')
        driver=webdriver.Firefox(options=options,executable_path=r'Y:\geckodriver.exe')
        return driver
    
    def stringdate_toDate(self,date=None):
        if date==None:
            date = str(datetime.datetime.now()).split()[0].split('-')
            year = date[0]
            month = date[1]
            day = date[2]
            return '{mm}/{dd}/{yyyy}'.format(mm=month,dd=day,yyyy=year)
        month_list = ['January','February','March','April','May','June','July','August','September','October','November','December']
        month_dict = dict((y,x+1) for x,y in enumerate(month_list))
        month = date.replace(',','').split()[0]
        day = date.replace(',','').split()[1]
        year = date.replace(',','').split()[2]
        for i in month_list:
            if i.startswith(str(month)):
                month = month_dict[i]
        return '{mm}/{dd}/{yyyy}'.format(mm=month,dd=day,yyyy=year)
    
    def inq_setNewsData(self,company_name,inq_main,driver=None):
        inq_link = inq_main.format(company_n=company_name)
        cycle_pages = []
        myDriver = driver if driver != None else self.setSelenium()
        myDriver.get(inq_link)
        page = BeautifulSoup(myDriver.page_source,'html.parser')
        mainpage = myDriver.current_url
        myDriver.quit()
        cycle_pages.append(page)
        navi=page.find('div',attrs={'class':'gsc-cursor'})
        time.sleep(2)
        for i,pages in enumerate(navi.find_all('div',attrs={'class':'gsc-cursor-page'})):
            time.sleep(6)
            myDriver2 = driver if driver != None else self.setSelenium()
            myDriver2.get(mainpage[:-1]+str(pages.text))
            page2 = BeautifulSoup(myDriver2.page_source,'html.parser')
            cycle_pages.append(page2)
            myDriver2.quit()
        data = []
        for i in cycle_pages:
            box=i.find('div',attrs={'class':'gsc-expansionArea'})
            mini_boxes = box.find_all('div',attrs={'class':'gsc-webResult gsc-result'})
            for i in mini_boxes:
                links = i.find('a')['href']
                title = i.find('a').text
                date = i.find('div',attrs={'class':'gs-bidi-start-align gs-snippet'}).text
                date_f = self.stringdate_toDate(date[:date.index('.')].strip())
                comp_name = company_name
                data.append({'news_link':links,'news_date_published':date_f,'news_title':title,'comp_name':comp_name})
        return data
    
    def extract_newsContent(self,comp_name,inq_link,driver=None):
        #add with the parameters comp_name
        #add a functionality to find comp
        content=''
        myDriver = driver if driver != None else self.setSelenium()
        myDriver.get(inq_link)
        page = BeautifulSoup(myDriver.page_source,'html.parser')
        myDriver.quit()
        container = page.find('div',attrs={'id':'article_content'})
        for i in container.find_all('p'):
            if i.text == "":
                continue
            content+=i.text
        return (comp_name,content)
    
    def mw_setStockData(self,mw_link,main_link,driver=None):
        pages = self.page_count
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
        cur_price1 = page.find('h2',attrs={'class':'intraday__price'})
        cur_status1 = page.find('div',attrs={'class':'element element--intraday'})
        cur_volume1 = page.find('div',attrs={'class':'range__header'})
    
        cur_status = cur_status1.find('div',attrs={'class':'status'}).text
        cur_price = cur_price1.find('span',attrs={'class':'value'}).text
        cur_volume = cur_volume1.find('span',attrs={'class':'primary'}).text
        cur_lastPrice = cur_status1.find('td',attrs={'class':'table__cell u-semi'}).text
    
        data_current = {
            'overview_date':self.stringdate_toDate(),
            'overview_statusNow':cur_status,
            'overview_priceNow':cur_price,
            'overview_volumeNow':cur_volume,
            'overview_lastpriceNow':cur_lastPrice
        }
        for i in over_list.find_all('li'):
            try:
                title = i.find('small',attrs={'class':'label'}).text
                data = i.find('span',attrs={'class':'primary'}).text
            except AttributeError:
                continue
            data_current['overview_'+title.replace(' ','_').lower().replace('/', '').replace('%','percent').replace('.','').replace('-','_')] = data
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
                    fin_data[head+'_'+title_holder.replace(' ','_').lower().replace('/','').replace('-','').replace('(','').replace(')','')] = x.text
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
                open_p = i.find_all('td')[1].find('div').text[1:]
                high_p = i.find_all('td')[2].find('div').text[1:]
                low_p = i.find_all('td')[3].find('div').text[1:]
                close_p = i.find_all('td')[4].find('div').text[1:]
                volume = i.find_all('td')[5].find('div').text
                chg
            except AttributeError:
                continue
            data_full = {
                'date':date,
                'open_price':float(open_p),
                'high_price':float(high_p),
                'low_price':float(low_p),
                'closed_price':float(close_p),
                'volume':int(volume)
            }
            histQuotes.append(data_full)
        return histQuotes
    
if __name__=='__main__':
    app = dataHandler()