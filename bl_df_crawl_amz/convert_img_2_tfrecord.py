import apache_beam as beam
import os
import datetime
import numpy as np
import io
import tensorflow as tf
import google.auth

from google.cloud import storage
from PIL import Image
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from tensorflow.python.framework import errors
from tensorflow.python.lib.io import file_io

PROJECT = os.environ['PROJECT']
DES_BUCKET = os.environ['DES_BUCKET']
SRC_DIR_PRD = os.environ['SRC_DIR_PRD']
DES_DIR_PRD = os.environ['SRC_DIR_PRD']
SRC_DIR_DEV = os.environ['SRC_DIR_PRD']
DES_DIR_DEV = os.environ['SRC_DIR_PRD']
PYTHONIOENCODING='UTF-8'
LOCAL_TMP_DIR='/tmp/'
DEV_MODE=False

# set service account file into OS environment value
job_name = 'cifar-10' + datetime.datetime.now().strftime('%y%m%d%H%M%S')

options = {
  'staging_location': 'gs://' + DES_BUCKET + '/staging',
  'temp_location': 'gs://' + DES_BUCKET + '/tmp',
  'job_name': job_name,
  'project': PROJECT,
  'zone': 'asia-northeast1-c',
  'teardown_policy': 'TEARDOWN_ALWAYS',
  'no_save_main_session': True,
  'requirements_file': 'requirements.txt',
  'save_main_session': True
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)

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


# def ImageToTfRecord(imagefile,label):

def run():
  p = beam.Pipeline(RUNNER, options=opts)
  l = (p
       | 'Read CSV' >> ReadFromText(inputfiles)
       | 'Parse CSV' >> beam.Map(parseCSV)
       | 'ReadImage' >> beam.Map(readImage)
       | 'Convert Image and Label to tf.train.example' >> beam.ParDo(TFExampleFromImageDoFn())
       | 'Serialized to String' >> beam.Map(lambda x: x.SerializeToString())
       | 'Save To Disk' >> beam.io.WriteToTFRecord(DES_DIR + 'cifar-10', file_name_suffix='.tfrecord')
       )
  job = p.run()
  job.wait_until_finish()


run()
print('Done')