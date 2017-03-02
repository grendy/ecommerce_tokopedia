import scrapy
from kafka import KafkaProducer,KafkaConsumer
from selenium import webdriver
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.keys import Keys
from scrapy.http import TextResponse
import traceback
import MySQLdb
from pyvirtualdisplay import Display
from selenium.webdriver.common.proxy import *
import setting
import demjson
import json
import time

class feedbackkafka:
    def __init__(self):
        self.conn = MySQLdb.connect(
            host=setting.host,
            port=setting.port,
            user=setting.user,
            passwd=setting.passwd,
            db=setting.db)
        self.connect = self.conn
        # path_to_chromedriver = 'D://chromedriver'
        # self.driver = webdriver.Chrome(executable_path = path_to_chromedriver)
        # self.driver = webdriver.Chrome()
        # driver.get("http://www.google.com")
        # service_args = [setting.proxy]
        # self.driver = webdriver.PhantomJS(service_args = service_args)
        myProxy = setting.firefox_proxy
        proxy = Proxy({
            'proxyType': ProxyType.MANUAL,
            'httpProxy': myProxy,
            'ftpProxy': myProxy,
            'sslProxy': myProxy,
        })
        display = Display(visible=0, size=(800, 600))
        display.start()
        self.driver = webdriver.Firefox(proxy=proxy)
    def parse(self):
        cur = self.conn.cursor()
        cou = self.conn.cursor()
        try:
            # import pdb;pdb.set_trace()
            count = "select count(*)from product_url where status_feed = ''"
            sql = "select product_url from product_url where status_feed = ''"
            cur.execute(sql)
            cou.execute(count)
            results = cur.fetchall()
            b = cou.fetchall()
            terus = str(b).replace(",", "").replace("'", "").replace("(", "").replace(")", "").replace("[", "").replace(
                "]", "").replace("L", "")
            print (terus)
            terus = int(terus)
            print "============================================"
            print (terus)
            for ulang in range(0, terus):
                try:
                    print (ulang)
                    a = results[ulang]
                    url = str(a).replace(",", "").replace("'", "").replace("(", "").replace(")", "")
                    # print(url)
                    print "====================================="
                    status_feed = "done"
                    sql = "UPDATE product_url SET status_feed = '{}' WHERE product_url = '{}'".format(status_feed,url)
                    cur.execute(sql)
                    self.conn.commit()
                    try:
                        # import pdb;pdb.set_trace()      #buat trace
                        url = url + "/review"
                        self.driver.get(url)
                        halaman = 0
                        for ulangi in range(0, 1000):
                            response = TextResponse(url=url, body=self.driver.page_source, encoding='utf-8')
                            for baris in range(1, 11):
                                halaman = halaman + 1
                                try:
                                    feed_nama = response.xpath('//ul[@class="list-box"]/li[' + str(baris) + ']/div/div/div[2]/small[1]/a/text()').extract_first()
                                    tanggal = response.xpath('//ul[@class="list-box"]/li[' + str(baris) + ']/div/div/div[2]/div[1]/small/i/time-ago/text()').extract_first()
                                    pesan = response.xpath('//ul[@class="list-box"]/li[' + str(baris) + ']/div/div/div[3]/span/text()').extract_first()
                                    product_url = self.driver.current_url
                                    penjual_url = MySQLdb.escape_string(response.xpath('//*[@id="shop-name-info"]/@href').extract_first())
                                    product_url = product_url.replace("/review", "")
                                    akhir = json.dumps({'type': 'feedback', 'feed_nama': feed_nama, 'tanggal': tanggal, 'pesan': pesan,'product_url': product_url, 'penjual_url': penjual_url})
                                    if feed_nama == None:
                                        break
                                    else:
                                        for kaf in range(0, 20):
                                            try:
                                                prod = KafkaProducer(bootstrap_servers=setting.broker)
                                                prod.send(setting.kafka_topic, b"{}".format(akhir))
                                                print "=================================================="
                                                print "SUKSES SEND TO KAFKA"
                                                print "=================================================="
                                                print akhir
                                                kaf = 1
                                            except:
                                                pass
                                            if kaf == 1:
                                                break
                                except Exception, e:
                                    print e
                                    print url
                            self.driver.find_element_by_xpath('/html/body').send_keys(Keys.END + Keys.PAGE_UP)
                            time.sleep(5)
                            try:
                                self.driver.find_element_by_css_selector('#next-page').click()
                                time.sleep(5)
                            except:
                                break
                    except Exception,e:
                        print e
                        print url
                except Exception,e:
                    print e
                    break
        except Exception, e:
            print e
if __name__ == '__main__':
    p = feedbackkafka()
    p.parse()