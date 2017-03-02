import scrapy
from logging import exception

from kafka import KafkaProducer,KafkaConsumer
from selenium import webdriver
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from scrapy.http import TextResponse
from pyvirtualdisplay import Display
from impala.dbapi import connect
import traceback
import MySQLdb
from selenium.webdriver.common.proxy import *
import setting
import sys
import json

import time
class producer:
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
            count = "select count(*)from product_url where status = ''"
            sql = "select product_url from product_url where status = ''"
            cur.execute(sql)
            cou.execute(count)
            results = cur.fetchall()
            b = cou.fetchall()
            terus = str(b).replace(",", "").replace("'", "").replace("(", "").replace(")", "").replace("[", "").replace("]", "").replace("L","")
            print (terus)
            terus = int(terus)
            print "============================================"
            print (terus)
            for ulang in range(0, terus):
                try:
                    print (ulang)
                    a = results[ulang]
                    url = str(a).replace(",", "").replace("'", "").replace("(", "").replace(")", "")
                    self.driver.get(url)
                    print "====================================="
                    response = TextResponse(url=url, body=self.driver.page_source, encoding='utf-8')
                    try:
                        # import pdb;pdb.set_trace()
                        product_url = url
                        penjual_url = MySQLdb.escape_string(
                            response.xpath('//*[@id="shop-name-info"]/@href').extract_first())
                        kategori = MySQLdb.escape_string(
                            response.xpath('//*[@id="breadcrumb-container"]/ul/li[3]/h2/a/text()').extract_first())
                        kategori_url = MySQLdb.escape_string(
                            response.xpath('//*[@id="breadcrumb-container"]/ul/li[3]/h2/a/@href').extract_first())
                        try:
                            nama_product = MySQLdb.escape_string(response.xpath(
                                '//*[contains(@id,"product")]/div[2]/div[1]/div[1]/h1/a/text()').extract_first())
                        except:
                            nama_product = MySQLdb.escape_string(response.xpath(
                                '//*[contains(@id,"product-")]/div[1]/div[1]/div[1]/h1/a/text()').extract_first())
                        harga = response.xpath(
                            '//*[contains(@id,"product")]/div[2]/div[2]/div[1]/div/div[1]/span[2]/text()').extract_first()
                        kondisi = MySQLdb.escape_string(
                            response.xpath('//*[contains(@id,"shop")]/div/div/div/dl/dd[5]/text()').extract_first())
                        berat = MySQLdb.escape_string(
                            response.xpath('//*[contains(@id,"shop")]/div/div/div/dl/dd[2]/text()').extract_first())
                        update_terakhir = response.xpath(
                            '//*[contains(@id,"product")]/div[2]/div[2]/div[1]/div/div[2]/small/i/text()').extract_first()
                        deskripsi = response.xpath('//*[contains(@id,"shop")]/p/text()').extract()
                        dilihat = response.xpath(
                            '//*[contains(@id,"shop")]/div/div/div/dl/dd[1]/text()').extract_first()
                        # // *[ @ id = "shop-540994"] / div / div / div / dl / dd[1]
                        terjual = response.xpath(
                            '//*[contains(@id,"shop")]/div/div/div/dl/dd[3]/text()').extract_first()
                        penjual = MySQLdb.escape_string(
                            response.xpath('//*[@id="shop-name-info"]/text()').extract_first())
                        lokasi = MySQLdb.escape_string(response.xpath(
                            '//*[@id="b-p-info-penjual"]/div[2]/div[1]/div[2]/div[4]/span/span/span/text()').extract_first())
                        produk_terjual = response.xpath(
                            '//*[@id="b-p-info-penjual"]/div[2]/div[3]/div/div[2]/div[1]/strong/text()').extract_first()
                        try:
                            if "," in produk_terjual:
                                produk_terjual = produk_terjual.replace("rb", "00").replace(",", "")
                            else:
                                produk_terjual = produk_terjual.replace("rb", "000")
                            produk_terjual = int(produk_terjual)
                        except:
                            produk_terjual = 0
                        try:
                            harga = harga.replace(".", "")
                        except:
                            harga = response.xpath(
                                '//*[contains(@id,"product-")]/div[1]/div[2]/div[1]/div/div[1]/span[2]/text()').extract_first()
                            harga = harga.replace(".", "")
                        if "." in berat:
                            berat = berat.replace(".", "").replace("gr", "").replace("\\n", "").strip()
                        else:
                            berat = berat.replace("gr", "").replace("\\n", "").strip()
                        kondisi = kondisi.replace("\\n", "").strip()
                        deskripsi = map(lambda s: s.strip(), deskripsi)
                        deskripsi = map(str, deskripsi)
                        deskripsi = ''.join(map(str, deskripsi))
                        try:
                            if "," in dilihat:
                                dilihat = dilihat.replace("rb", "00").replace(",", "").replace("\n", "")
                            else:
                                dilihat = dilihat.replace("rb", "000").replace("\n", "")

                        except:
                            dilihat = 0
                        dilihat = int(dilihat)
                        try:
                            if "," in terjual:
                                terjual = terjual.replace("rb", "00").replace(",", "").replace("\n", "").replace("\n",
                                                                                                                 "")
                            else:
                                terjual = terjual.replace("rb", "000").replace("\n", "")
                        except:
                            terjual = 0
                        terjual = int(terjual)
                        try:
                            update_terakhir = update_terakhir.replace("Perubahan Harga Terakhir:", "")
                        except:
                            update_terakhir = response.xpath(
                                '//*[contains(@id,"product-")]/div[1]/div[2]/div[1]/div/div[2]/small/i/text()').extract_first()
                            update_terakhir = update_terakhir.replace("Perubahan Harga Terakhir:", "")
                        harga = int(harga.encode('utf-8'))
                        product = json.dumps({'type': 'product', 'product_url': product_url, 'penjual_url': penjual_url,
                                              'kategori': kategori, 'kategori_url': kategori_url,
                                              'nama_product': nama_product, 'harga': harga, 'kondisi': kondisi,
                                              'dilihat': dilihat, 'terjual': terjual, 'berat': berat,
                                              'update_terakhir': update_terakhir, 'deskripsi': deskripsi, 'penjual':penjual,
                                              'lokasi':lokasi, 'product_terjual':produk_terjual})
                        for kaf in range(0, 20):
                            try:
                                prod = KafkaProducer(bootstrap_servers=setting.broker)
                                prod.send(setting.kafka_topic, b"{}".format(product))
                                print "=================================================="
                                print "SUKSES SEND TO KAFKA"
                                print "=================================================="
                                print product
                                status = "done"
                                sql = "UPDATE product_url SET status = '{}' WHERE product_url = '{}'".format(status,url)
                                cur.execute(sql)
                                self.conn.commit()
                                kaf = 1
                            except:
                                pass
                            if kaf == 1:
                                break
                    except Exception, e:
                        print e
                        print url
                    # try:
                    #     for kaf in range(0, 15):
                    #         prod = KafkaProducer(bootstrap_servers='master01.cluster1.ph:6667')
                    #         prod.send('ecommerce', b"{}".format(url))
                    #         time.sleep(70)
                    #         break
                    # except:
                    #     pass
                except Exception, e:
                    print e
        except Exception, e:
            print e
        cur.close()
        try:
            self.driver.close()
        except Exception, e:
            print e

if __name__ == '__main__':
    p = producer()
    p.parse()

