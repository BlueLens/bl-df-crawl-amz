from __future__ import absolute_import
from __future__ import print_function

import redis
import os
import pickle
import logging
import logging
from stylelens_crawl_amazon.item_search import ItemSearch
from stylelens_crawl_amazon.model.item_search_data import ItemSearchData
from stylelens_crawl_amazon.stylelens_crawl import StylensCrawler
from bluelens_log import Logging

REDIS_CRAWL_AMZ_QUEUE = "bl:crawl:amz:queue"
REDIS_TICKER_KEY = "bl:ticker:crawl:amazon"


class Crawler(object):
  def __init__(self, args):
    super(Crawler, self).__init__()
    logging.info('Crawler: __init__ start')
    self._crawler = StylensCrawler(AWSAccessKeyId=args.AWS_ACCESS_KEY_ID,
                                   AWSSecretAccessKey=args.AWS_SECRET_ACCESS_KEY,
                                   AssociateTag=args.AWS_ASSOCIATE_TAG,
                                   Region=args.AWS_REGION)
    self._rconn = redis.StrictRedis(args.REDIS_SERVER, port=6379, password=args.REDIS_PASSWORD)
    logging.info('Crawler: __init__ done')

  def wait_tick(self):
    self._rconn.blpop([REDIS_TICKER_KEY])

  def get_search_keyword(self):
    logging.info('get_search_keyword')
    value = self._rconn.lpop(REDIS_CRAWL_AMZ_QUEUE)
    if value == None:
      return None
    else:
      search_keyword = pickle.loads(value)
      return search_keyword

  def do(self, search_keyword):
    print('do')
    logging.info('Crawler: do start')
    item_search = ItemSearch()
    item_search.search_data = ItemSearchData().from_dict(search_keyword)
    print(item_search.search_data)
    items = self._crawler.get_items([item_search])

    products = self.get_products(items, 'aaa', 'bbb')
    logging.info('products: ' + str(products))
    logging.info('Crawler: do end')
    return products

  def get_products(self, items, host_code, host_group):
    products = []

    for item in items:
      try:
        if item is None:
          return False
        logging.info(item)
        product = item.to_dict()
        product['name'] = item.title
        product['host_url'] = 'https://www.amazon.com'
        product['host_code'] = host_code
        product['host_group'] = host_group
        product['host_name'] = 'amazon'
        product['product_no'] = item.asin
        if product.get('features') != None:
          product['features'] = str(product.get('features'))
        # price_dic = product.get('price')
        # if price_dic != None:
        #   product['price']['amount'] = int(price_dic.get('amount'))
        #   product['price']['currency_code'] = price_dic.get('currency_code')
        #   product['price']['formatted_price'] = price_dic.get('formatted_price')
        # lowest_price_dic = product.get('lowest_price')
        # if lowest_price_dic != None:
        #   product['lowest_price']['amount'] = int(lowest_price_dic.get('amount'))
        #   product['lowest_price']['currency_code'] = lowest_price_dic.get('currency_code')
        #   product['lowest_price']['formatted_price'] = lowest_price_dic.get('formatted_price')
        # highest_price_dic = product.get('highest_price')
        # if highest_price_dic != None:
        #   product['highest_price']['amount'] = int(highest_price_dic.get('amount'))
        #   product['highest_price']['currency_code'] = highest_price_dic.get('currency_code')
        #   product['highest_price']['formatted_price'] = highest_price_dic.get('formatted_price')
        #
        # if price_dic == None and lowest_price_dic == None:
        #   continue
        product['main_image'] = item.l_image.url
        # product['sub_images'] = item['sub_images']
        # product['sub_images'] = None
        product['version_id'] = 'version_xxx'
        product['product_url'] = item.detail_page_link
        # product['tags'] = item.features
        product['nation'] = 'us'
        products.append(product)
      except Exception as e:
        # log.error("Exception when calling ProductApi->add_products: %s\n" % e)
        return None

    return products