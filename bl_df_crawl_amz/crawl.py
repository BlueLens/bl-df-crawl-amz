from __future__ import absolute_import

import apache_beam as beam
import os
import datetime
import numpy as np
import io
import tensorflow as tf
import google.auth
import logging
import argparse

from google.cloud import storage
from PIL import Image
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from .crawler import Crawler

from tensorflow.python.framework import errors
from tensorflow.python.lib.io import file_io

PROJECT = os.environ['PROJECT']
DES_BUCKET = os.environ['DES_BUCKET']
SRC_DIR_PRD = os.environ['SRC_DIR_PRD']
DES_DIR_PRD = os.environ['SRC_DIR_PRD']
SRC_DIR_DEV = os.environ['SRC_DIR_PRD']
DES_DIR_DEV = os.environ['SRC_DIR_PRD']
PYTHONIOENCODING='UTF-8'
# OUTPUT = PROJECT + ':.test'
LOCAL_TMP_DIR='/tmp/'
DEV_MODE=False

# set service account file into OS environment value
job_name = 'crawl' + datetime.datetime.now().strftime('%y%m%d%H%M%S')

crawler = None

if (DEV_MODE):
  RUNNER = 'DirectRunner'
  inputfiles = SRC_DIR_DEV + '/cifar.csv'
  SRC_DIR = SRC_DIR_DEV
  DES_DIR = DES_DIR_DEV + '/'
else:
  inputfiles = SRC_DIR_PRD + '/cifar.csv'
  SRC_DIR = SRC_DIR_PRD
  DES_DIR = DES_DIR_PRD + '/'
  RUNNER = 'DataflowRunner'


# Apache beam functions
def parseCSV(element):
  line = 'image0.png,1'
  e = line.split(',')
  filename = str(e[0])
  label = int(e[1])
  return filename, label

def get_search_keywords(element):
  global crawler
  logging.info('get_search_keywords')
  while True:
    search_keyword = crawler.get_search_keyword()
    if search_keyword == None:
      break
    else:
      yield search_keyword

def readImage(element):
  filename, label = element

  filepath = ''
  if (DEV_MODE):
    filepath = SRC_DIR + '/' + filename
  else:
    # download file from gcs to local
    storageClient = storage.Client()
    source_bucket = storageClient.get_bucket(DES_BUCKET)
    blob = source_bucket.get_blob('data/images/' + filename)

    # 1) download file
    filepath = LOCAL_TMP_DIR + filename
    with open(filepath, 'wb') as file_obj:
      blob.download_to_file(file_obj)

  print('[MYLOG] read image :' + filepath)
  image = open(filepath, 'rb')
  # image_bytes = image.read()
  # img = np.array(Image.open(io.BytesIO(image_bytes)).convert('RGB'))
  # img_raw = img.tostring()
  # print img_raw
  bytes = image.read()
  image.close()

  # if it is running over dataflow, delete temp file
  if (DEV_MODE == False):
    os.remove(filepath)

  return bytes, label


class CrawlDoFn(beam.DoFn):
  def __init__(self, args):
    print('CrawlDoFn')
    self.args = args
    print(args.REDIS_SERVER)
    print(args.AWS_ACCESS_KEY_ID)
    print(args.AWS_SECRET_ACCESS_KEY)
    print(args.AWS_ASSOCIATE_TAG)
    print(args.AWS_REGION)

  def process(self, element):

    logging.info('element: ' + str(element))
    logging.info(element)
    crawler = Crawler(self.args)
    products = crawler.do(element)

    if products == None or isinstance(products, bool):
      pass
    else:
      for p in products:
        yield p

def save(element):
  logging.info('saving : %s', element)


# def ImageToTfRecord(imagefile,label):

class TFExampleFromImageDoFn(beam.DoFn):
  def process(self, element):
    def _bytes_feature(value):
      return tf.train.Feature(bytes_list=tf.train.BytesList(value=value))

    def _float_feature(value):
      return tf.train.Feature(float_list=tf.train.FloatList(value=value))

    def _int64_feature(value):
      return tf.train.Feature(int64_list=tf.train.Int64List(value=value))

    try:
      element = element.element
    except AttributeError:
      pass
    bytes, label = element

    example = tf.train.Example(features=tf.train.Features(feature={
      'image_raw': _bytes_feature([bytes]),
      'label': _int64_feature([label])
    }))

    yield example

def get_search_keywords(pipeline):

  def get():
    crawler = Crawler()
    yield crawler.get_search_keyword()

  keywords = (pipeline
              | 'add points' >> beam.Create(get()))

  return keywords

def run(argv=None):
  parser = argparse.ArgumentParser()
  parser.add_argument('--output',
                      dest='output',
                      required=True,
                      help='Output file to write results to.')
  parser.add_argument('--REDIS_PASSWORD',
                      dest='REDIS_PASSWORD',
                      required=True,
                      help='')
  parser.add_argument('--REDIS_SERVER',
                      dest='REDIS_SERVER',
                      required=True,
                      help='')
  parser.add_argument('--AWS_SECRET_ACCESS_KEY',
                      dest='AWS_SECRET_ACCESS_KEY',
                      required=True,
                      help='')
  parser.add_argument('--AWS_ACCESS_KEY_ID',
                      dest='AWS_ACCESS_KEY_ID',
                      required=True,
                      help='')
  parser.add_argument('--AWS_ASSOCIATE_TAG',
                      dest='AWS_ASSOCIATE_TAG',
                      required=True,
                      help='')
  parser.add_argument('--AWS_REGION',
                      dest='AWS_REGION',
                      required=True,
                      help='')
  known_args, pipeline_args = parser.parse_known_args(argv)
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  p = beam.Pipeline(RUNNER, options=pipeline_options)


  query_keywords = 'select * from bluelens.amazon_search_keywords where index=$index'

  keywords = p | 'Read' >> beam.io.Read(beam.io.BigQuerySource(query=query_keywords))

  (keywords | 'Crawl' >> beam.ParDo(CrawlDoFn(known_args))
       | 'write' >> beam.io.Write(beam.io.BigQuerySink('products',
                                                       dataset='bluelens',
                                                       write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
  )
  job = p.run()
  # job.wait_until_finish()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
