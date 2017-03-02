import scrapy
from selenium import webdriver
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from scrapy.http import TextResponse
from tokorobet.items import tcategory
from impala.dbapi import connect
import traceback
import MySQLdb
from pyvirtualdisplay import Display

import time


class ProductSpider(scrapy.Spider):
    name = "kategori"
    allowed_domains = ["https://tokopedia.com"]
    start_urls = ["https://www.tokopedia.com"]

    def __init__(self,conn):
        self.conn = conn
        # path_to_chromedriver = 'D://chromedriver'
        # self.driver = webdriver.Chrome(executable_path = path_to_chromedriver)
        # self.driver = webdriver.PhantomJS()
        display = Display(visible=0, size=(800,600))
        display.start()
        self.driver = webdriver.Firefox()

    @classmethod
    def from_crawler(cls, crawler):
        conn = MySQLdb.connect(
            host=crawler.settings['MYSQL_HOST'],
            port=crawler.settings['MYSQL_PORT'],
            user=crawler.settings['MYSQL_USER'],
            passwd=crawler.settings['MYSQL_PASS'],
            db=crawler.settings['MYSQL_DB'])
        return cls(conn)

    def parse(self, response):
        cur = self.conn.cursor()
        try:
            a = 0
            for c in range(0, 2):
                a = a + 1
                b = 0
                for sc in range(0, 7):
                    b = b + 1
                    url = 'https://www.tokopedia.com'
                    try:
                        # import pdb;pdb.set_trace()
                        self.driver.get(url)
                    except:
                        print traceback.print_exc()
                    time.sleep(10)
                    for tidur in range(0, 100):
                        time.sleep(1)
                        try:
                            for kat in range(0,3):
                                a = 0
                                for coy in range(0,7):
                                    a = a+1
                                    response = TextResponse(url=response.url, body=self.driver.page_source, encoding='utf-8')
                                    url = response.xpath('//*[@id="content-container"]/section[3]/div/div[2]/div/ul['+str(kat+1)+']/li['+str(a)+']/a/@href').extract_first()
                                    nama_kategori = response.xpath('//*[@id="content-container"]/section[3]/div/div[2]/div/ul['+str(kat+1)+']/li['+str(a)+']/a/span/text()').extract_first()
                                    time.sleep (2)
                                    print "========================================"
                                    print(nama_kategori)
                                    print(url)
                                    print "========================================"
                                    sql = "select * from tokopedia_kategori where url = '{}' and nama_kategori = '{}'".format(url, nama_kategori)
                                    cur.execute(sql)
                                    results = cur.fetchall()
                                    if len(results) == 0:
                                        sql = "INSERT INTO tokopedia_kategori VALUES ('{}','{}')".format(url, nama_kategori)
                                        print sql
                                        cur.execute(sql)
                                        self.conn.commit()
                                        print "======================================"
                                        print "[INFO] impala insert sukses : {}".format(sql)
                                        print "======================================"
                                    else:
                                        print "======================================"
                                        print "[ERROR] impala insert failure : {}".format(sql)
                                        print "============s=========================="
                        except:
                            pass
                    # for click in range(0, 240):
                    #     response = TextResponse(url=response.url, body=self.driver.page_source, encoding='utf-8')
                    #     c = 0
                    #     if a == 2 and b == 1:
                    #         b = b + 1
                    #     else:
                    #         for barang in range(0, 25):
                    #             c = c + 1
                    #             if c == 1:
                    #                 pass
                    #             else:
                    #                 try:
                    #                     self.driver.get(url)
                    #                     time.sleep(5)
                    #                 except:
                    #                     print traceback.print_exc()
                    #             for tidu in range(0, 100):
                    #                 time.sleep(1)
                    #                 try:
                    #                     self.driver.find_element_by_xpath('//*[@id="content-directory"]/div[2]/div[' + str(c) + ']/a').click()
                    #                     time.sleep(5)
                    #                     break
                    #                 except:
                    #                     print traceback.print_exc()
                    #             time.sleep(30)
        except:
            pass
        cur.close()
        try:
            self.driver.close()
        except:
            pass