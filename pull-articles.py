#!/usr/bin/env python3

import feedparser
import csv
import gzip
import sys
import re
import os
import json
import requests
import sqlite3
import datetime
from bs4 import BeautifulSoup

cat_file = 'categories.csv'

drop_path = 'articles'
article_db = 'seen_articles.db'

# One table per category, so that when we have millions 
# of articles, we don't have to churn through millions of
# records to find out if we already have an article. 
#
# We may want to roll the database annually (or more frequently?) 
# as well, to prevent tons and tons of antique useless metadata
CAT_SCHEMA = """
  CREATE TABLE IF NOT EXISTS "%s" (
    id integer PRIMARY KEY,
    link NOT NULL,
    date text,
    title text,
    published text,
    summary text,
    language text,
    contributors text,
    publisher text,
    stripped_text text
  );
"""

# We technically store the link twice: once in the full category table
# but also in a 'recent_links' table, which keeps all of the links
# seen in the last week. This is to prevent chugging through the entire
# database whenever we want to see if we've already grabbed a link. Most
# of the time, we'll be repeating links because we're pulling the RSS
# feed again and again. The 'have we seen this link' test should return
# fast, since we'll be running it a lot, so we'll test against this table.
RECENT_SCHEMA = """
  CREATE TABLE IF NOT EXISTS recent_links (
    id integer PRIMARY KEY,
    link text,
    date text
  );

  CREATE UNIQUE INDEX idx_links_link on recent_links (link);
"""

def _create_recent_schema(cur):
  cur.executescript(RECENT_SCHEMA)
  db.commit()

def _check_db_table_exists(cur, table):
  """
  Returns True if table exists, False if not
  """
  cur.execute("SELECT name from sqlite_master where type='table' AND name='%s';" % table)
  if len(cur.fetchall()) == 0:
    return False
  else:
    return True

def _create_db_category(cur, topic):
  """
  Sets up the database schema

  @param: cur Database cursor
  """
  # the CAT_SCHEMA names the table after the topic
  cur.executescript(CAT_SCHEMA % str(topic))
  db.commit()
  


def _record_exists(cur, url):
  """
  Internal method to see if a database entry exists. 

  @param: cur Cursor for database
  @param: url URL to check the existence of

  Returns True if the URL is found, False if not.
  """
  cur.execute('SELECT * FROM recent_links where link = "%s";' % url)
  if len(cur.fetchall()) == 0:
    return False
  else:
    return True


def record_article(cur, article, topic):
  """
    Takes an article entry, checks in the database to see if we've already seen it, 
    and if not, pulls down the article, saves it in the proper folder, records the 
    metdata. 

    @param: cur Cursor for database work
    @param: article A dictionary of key/values from the RSS feed

    Returns: True for success, False for fail
  """
  if _record_exists(cur, article['link']):
    return False

  if not _check_db_table_exists(cur, topic):
    print("Creating %s DB table" % topic)
    _create_db_category(cur, topic)

  # The summaries often have useless html that we don't want or need. 
  summary = BeautifulSoup(article.get('summary'),features="lxml").get_text(strip=True)

  print("Pulling %s" % article.get('link'))
  req = requests.get(article.get('link'))

  # We're going to do text analysis on the contents of the page, and that's much smaller
  # than the full html output, so lets save it to the database. 
  stripped_text = BeautifulSoup(req.text, features="lxml").get_text(strip=True)
  
  # Technically, the topic is still open to injection, but we're pulling that directly out of 
  # a CSV we control, so I think we can consider it secure. The plumbing required to 
  # dynamically pick a table is otherwise annoying. 
  cur.execute("""
    INSERT INTO "%s" (link, date, title, published, summary, language, contributors, publisher, stripped_text) 
    VALUES 
    (?, ?, ?, ?, ?, ?, ?, ?, ?);""" % topic, [ 
      article.get('link'), 
      str(datetime.datetime.now()), 
      article.get('title'), 
      article.get('published'), 
      summary, 
      article.get('language'), 
      str(article.get('contributors')), 
      article.get('publisher'),
      stripped_text,
      ]
    )
  cur.execute("""
    INSERT INTO recent_links (link, date) 
    VALUES 
    (?, ?);""", [
      article.get('link'), 
      str(datetime.datetime.now())
    ]
    )
  db.commit()

  try:
    published = re.sub(r'\W+', ':', article.get('published'))
    title = re.sub(r'\W+', '_', article.get('title'))
    filename = published.replace(' ', '_') + '_' + title.replace(' ', '_')
  except TypeError:
    print("Unable to perform regex replacement on %s" % article.get('title'))
    filename = str(datetime.datetime.now().timestamp())
  
  # Sometimes the summaries get out of hand, so cut it to 100 chars
  filename = filename[:100] + ".txt.gz"

  # We want to write the full html output to the file for posterity
  print("Writing file %s/%s/%s" % (drop_path, topic, filename))
  f = open("%s/%s/%s" % (drop_path, topic, filename), 'a')
  with gzip.open("%s/%s/%s" % (drop_path, topic, filename), "wt") as f:
    f.write(req.text)
  
# Eventually may want to use something besides sqlite if 
# we unify scraping.
# sqlite3.connect() defaults to 'rwc' mode, which creates the database
db = sqlite3.connect(article_db, isolation_level='IMMEDIATE')
cur = db.cursor()

with open(cat_file) as f:
  reader = csv.reader(f)

  # Lets make sure that the recent_links table exists 
  if not _check_db_table_exists(cur, 'recent_links'):
    print("Creating the recent_links schema")
    _create_recent_schema(cur)

  for row in reader:
    print('-------------')
    if not os.path.isdir(drop_path + '/' + row[1]):
      os.mkdir(drop_path + '/' + row[1])
    rssurl = row[2]
    feed = feedparser.parse(rssurl)
    if feed['bozo']:
      print('Warning: This feed is malformed and may misbehave: %s' % row[2])
      print('Skipping.')
      continue
    try:
      print(f"Found {len(feed['items'])} items in {feed['feed']['title']} ({feed['feed']['subtitle']})")
    except KeyError as err:
      print("Line: %s" % row)
      print("Error: %s" % str(err))
      print("Found this:")
      print(str(feed))
      print(json.dumps(feed, indent=4, sort_keys=True))
      sys.exit(1)
    for item in feed['items']:
      record_article(cur, item, row[1])
cur.close()
