# encoding=utf8
# minghao_qiu 2017-10-27
import random
import re
import threading

import datetime
import Queue

import psycopg2
import pymongo
import time
import yaml
import logging

filehandler = logging.FileHandler('notification.log')


def create_logger(level=logging.INFO):
  """Create a logger according to the given settings by jerry"""

  logger = logging.getLogger("IPlogger")
  logger.setLevel(level)
  formatter = logging.Formatter('%(asctime)s  %(filename)s  %(levelname)s - %(message)s',
                                datefmt='%a, %d %b %Y %H:%M:%S', )
  filehandler.setFormatter(formatter)
  logger.addHandler(filehandler)
  return logger


config_file = yaml.load(open('config.yml'))
test = config_file['test_mongodb']
remote = config_file['mongodb']

remote_sql = config_file['postgresql']
test_sql = config_file['test_postgresql']

if config_file['debug']==True:
  now = test
  now_sql = test_sql
else:
  now = remote
  now_sql = remote_sql

logger = create_logger()
result = Queue.Queue()


def create_table(name):
  CREATE_TABLE = '''CREATE TABLE if not exists "''' + str(name) + '''" (
        "id" serial,
        "date" date,
        "local_price" numeric,
        "action" boolean,
         primary key(id,date))'''
  return CREATE_TABLE


def db_thread():
  while True:
    time.sleep(2)
    postgre_conn = psycopg2.connect(database=now_sql['database'],
                                    user=now_sql['user'],
                                    password=now_sql['password'],
                                    host=now_sql['host'],
                                    port=now_sql['port'])
    cur = postgre_conn.cursor()
    while result.empty() is False:
      try:
        cur.execute(result.get())
        postgre_conn.commit()
      except:
        postgre_conn.rollback()
    cur.close()
    postgre_conn.close()


def compare(id, day1, day2, db, result_day2=''):
  result_day1 = db['Airbnb-' + str(day1)].find_one({'_id': id}, {'unavailable': 1})
  if result_day2 == '':
    result_day2 = db['Airbnb-' + str(day2)].find_one({'_id': id}, {'unavailable': 1})
  result_dict1 = {}
  result_dict2 = {}

  if result_day1 and result_day2:
    for each in result_day1['unavailable']:
      result_dict1[each['date']] = each['local_price']
    for each in result_day2['unavailable']:
      result_dict2[each['date']] = each['local_price']
    for k, v in result_dict1.items():
      if not result_dict2.has_key(k) and datetime.datetime.strptime(k, '%Y-%m-%d') > datetime.datetime.strptime(day2,
                                                                                                                '%Y-%m-%d'):
        result.put(
          'INSERT INTO "' + str('Airbnb-' + day2) + '"("id", "date", "local_price","action") VALUES' + "('" + str(
            id) + "','" + str(k) + "','" + str(result_dict1[k]) + "',False)")
        # db3['a_' + day2].insert({"_id": id+k,"id":id,"action": 0,"date": k,
        #                        'local_price': result_dict1[k]})
    for k, v in result_dict2.items():
      if not result_dict1.has_key(k):
        result.put(
          'INSERT INTO "' + str('Airbnb-' + day2) + '"("id", "date", "local_price","action") VALUES' + "('" + str(
            id) + "','" + str(k) + "','" + str(result_dict2[k]) + "',True)")
        #   db3['a_' + day2].insert({"_id": id+k, "id": id, "action": 1, "date": k,
        #                          'local_price': result_dict2[k]})

    return [result_day1, result_day2]
  else:
    return [False, result_day2]


def sort_db(f, t):
  sorted_list = []
  conn_mongo = pymongo.MongoClient(now['host'], 27017)
  postgre_conn = psycopg2.connect(database=now_sql['database'],
                                  user=now_sql['user'],
                                  password=now_sql['password'],
                                  host=now_sql['host'],
                                  port=now_sql['port'])
  cur = postgre_conn.cursor()
  with conn_mongo:
    db = conn_mongo.Airbnb
    for each_collection in db.collection_names():
      crawl_date = re.findall('Airbnb-(.*?) ', each_collection + ' ', re.S)
      if crawl_date and datetime.datetime.strptime(f, '%Y-%m-%d') < datetime.datetime.strptime(crawl_date[0],
                                                                                               '%Y-%m-%d') and datetime.datetime.strptime(
              t, '%Y-%m-%d') > datetime.datetime.strptime(crawl_date[0], '%Y-%m-%d'):
        sorted_list.append(crawl_date[0])
        try:
          cur.execute(create_table('Airbnb-' + crawl_date[0]))
          postgre_conn.commit()
        except:
          postgre_conn.rollback()
  sorted_list = sorted(sorted_list, reverse=True)  # 小到大
  cur.close()
  postgre_conn.close()
  return sorted_list


def run_id(id, ls, db):
  big = 0
  small = 1
  result_day2 = ''
  for k in range(len(ls) - 1):
    result = compare(id, ls[small], ls[big], db, result_day2)
    if not result[0]:
      result_day2 = result[1]
      small = small + 1
    else:
      big = small
      small = small + 1
      result_day2 = result[0]


def run_history(f, t):
  id_list = []
  logger.warning('Start running')
  ls = sort_db(f, t)
  conn_mongo = pymongo.MongoClient(now['host'], 27017)
  with conn_mongo:
    conn_mongo2 = pymongo.MongoClient(now['host'], 27017)
    with conn_mongo2:
      conn = psycopg2.connect(database=now_sql['database'],
                                  user=now_sql['user'],
                                  password=now_sql['password'],
                                  host=now_sql['host'],
                                  port=now_sql['port'])
      cur = conn.cursor()
      db2 = conn_mongo2.Airbnb  # RAW DATA
      logger.warning('RAW DATA Connected')
      db = conn_mongo.Airbnb[now['roomid']].find() # RoomID DATA
      logger.warning('RoomID DATA Connected')

      for each in db:
        id_list.append(each['_id'])
      random.shuffle(id_list) #Randomize ID List
      logger.warning('List randomized')

      for each in id_list:
        try:
          cur.execute('SELECT id from "Airbnb_log" where id=\'' + str(each) + "'")
          rows = cur.fetchall()
          if len(rows) == 0:
            logger.warning(str(each)+'Run')
            run_id(each, ls, db2)
            cur.execute('INSERT INTO "Airbnb_log"("id") VALUES(' + str(each) + ')')
            conn.commit()
        except Exception as err:
          logger.error(err)


for num in range(3):
  threading.Thread(target=db_thread).start()
run_history(str(config_file['run']['from']), str(config_file['run']['to']))
