#!/usr/bin/python

import mysql.connector as mariadb
from mysql.connector import errorcode
from collections import OrderedDict
import os, fnmatch, sys

# REQUIREMENTS: MYSQL 5.7, Python2.7
# python ecs_challenge.py db_scripts ecs_user localhost ecs_database ecs_user

# statements/ queries
CREATE_VERSION_TABLE      = "CREATE TABLE versionTable(id INT AUTO_INCREMENT PRIMARY KEY, version INT)"
QUERY_GET_DB_VERSION      = "SELECT version FROM versionTable where id = 1"
QUERY_CHECK_TABLE_EXISTS  = "SELECT table_name FROM information_schema.tables WHERE table_name = '%s'"
SET_VERSION               = "INSERT INTO versionTable(version) VALUES('%d')"
UPDATE_VERSION_TABLE      = "UPDATE versionTable SET version = '%d' where id = 1"

# function for creating db connection
def get_db_connection(host, database, user, password):
  try:
    db = mariadb.connect(host=host, user=user, db=database, passwd=password)
    return db
  except mariadb.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
      print("Incorrect credentials")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
      print("Database does not exist")
    else:
      print(err)
    return None

# method for retrieving db version from version table
def get_db_version(db):
  cursor = db.cursor()
  cursor.execute(QUERY_GET_DB_VERSION)
  version = cursor.fetchone()
  if version != None:
    return version[0]
  return None

# check passed 'table_name' exists in db
def check_table_exists(db, table_name):
  cursor = db.cursor()
  query = QUERY_CHECK_TABLE_EXISTS % (table_name)
  cursor.execute(query)
  return cursor.fetchone()

# execute an sql file
def run_sql_file(db, sql):
  file = open(sql['sql_file'], 'r')
  sqlFile = file.read()
  sqlStatements = sqlFile.split(';')
  cursor = db.cursor()
  for statement in sqlStatements:
    if statement:
      try:
        print statement
        cursor.execute(statement)
        db.commit()
      except mariadb.Error as err:
        print "Failed to update database, rollback: {}".format(err)
        db.rollback()
        return False
  return True

# execute single sql statement and return True
def run_sql(db, sql):
  try:
    cursor = db.cursor()
    cursor.execute(sql)
    db.commit()
    return True
  except mariadb.Error as err:
    print "Failed to update database, rollback: {}".format(err)
    db.rollback()
    return False

# run required scripts for database update
def update_db(db, sql_files, db_version):
  current_version = db_version
  for key, value in sql_files.items():
    if run_sql_file(db, value):
      current_version = int(value['file_num'])
    else:
      print "Executing " + value['sql_file'] + " failed!"
      break
  return current_version

# get all sql files from passed dir
def get_sql_files(dir, db_version):
  files = os.listdir(str(dir))
  sql_files = {}
  for file in files:
    if fnmatch.fnmatch(file, "*.sql"):
      file_num = get_sql_file_number(file)
      if file_num != None:
        if int(file_num) > db_version:
          sql_files.update({ files.index(file): { 'file_num': int(file_num), 'sql_file': file} })
  return OrderedDict(sorted(sql_files.items(), key=lambda t: t[1]))

def get_sql_file_number(file):
  import re
  return re.search(r'\d+', file).group()

def main():
  
  print "===================\nBEGINNING DB UPDATE\n===================\n"
  
  scripts = sys.argv[1]
  db_config = {
    'user': sys.argv[2],
    'password': sys.argv[5],
    'host': sys.argv[3],
    'database': sys.argv[4]
  }
  
  # open database connection
  db = get_db_connection(**db_config)
  
  # if db connection was successsful, continue
  if db != None:
    
    # check if versionTable exists if not, create it
    if check_table_exists(db, "versionTable") == None:
      if run_sql(db, CREATE_VERSION_TABLE):
        print "Version datase table created!\n"
      
    # versionTable existed with value, scripts run prior
    if get_db_version(db) != None:
      # store current db version
      db_version = get_db_version(db)
    else:
      db_version = 0

    print "CURRENT DATABASE VERSION: " + str(db_version) + "\n"

    # grab sql files to execute
    sql_files = get_sql_files('./' + scripts, int(db_version))

    # sql files not empty, continue
    if sql_files:
      # TODO: DEAL WITH ABSOLUTE/ RELATIE PATHS?
      os.chdir(os.getcwd() + "/" + scripts)
      updated_version = update_db(db, sql_files, db_version)
      
      # update db version
      cursor = db.cursor()
      if db_version == 0:    
        cursor.execute(SET_VERSION % (int(updated_version)))
      else:
        cursor.execute(UPDATE_VERSION_TABLE % (int(updated_version)))
      db.commit()
    
    print "\n=================================\nDONE -- NEW DATABASE VERSION: " + str(get_db_version(db)) + "\n================================="

  db.close()
  
main()