# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
import re
import pymysql

def connect_database():
    return pymysql.connect(user='root',passwd='zjx261023')
def table_exists(cursor,table_name): 
    e_sql = 'show tables'
    try:
        cursor.execute(e_sql)
        tables = cursor.fetchall()
        table_list = [re.sub("['\,\(\)]", '', str(itable)) for itable in tables]
        if table_name in table_list:
            return 1#True
        else:
            print('table {} is not exists'.format(table_name))
            return 0#False
    except:
        print('{} is not exists'.format(table_name))
def database_exists(cursor,database_name): 
    e_sql = 'show databases'
    try:
        cursor.execute(e_sql)
        database_a = cursor.fetchall()
        database_list = [re.sub("['\,\(\)]", '', str(idatabase)) for idatabase in database_a]
        if database_name in database_list:
            return 1#True
        else:            
            print('database {} is not exists'.format(database_name))
            return 0#False
    except:
        pass   
    
def creat_database(cursor,DB_NAME):  
    e_sql = 'create database if not exists {}'.format(DB_NAME)
    try:
        cursor.execute(e_sql)
    except:
        pass
    
def creat_table(DB_NAME,table_name,table_format):
    db = connect_database()
    cursor = db.cursor()     
    try:
        if not database_exists(cursor,DB_NAME):
            creat_database(cursor,DB_NAME)
        e_sql = 'use {}'.format(DB_NAME)
        cursor.execute(e_sql) 
        if not table_exists(cursor,table_name):
            e_sql = '''create table {}({})'''.format(table_name,table_format)
            cursor.execute(e_sql)
            print('create table {}'.format(table_name))
        else:
            print('create failed, table {0} is exists, please drop table {0}'.format(table_name))
    except:
        db.rollback()
    db.close()
    
def drop_database(DB_NAME):
    db = connect_database()
    cursor = db.cursor()    
    e_sql = 'drop database if exists {}'.format(DB_NAME)
    try:
        cursor.execute(e_sql)
        print('drop_database function: drop database {}'.format(DB_NAME))
    except:
        db.rollback()
    db.close()    

def drop_table(DB_NAME,table_name):
    db = connect_database()
    cursor = db.cursor() 
    try:
        if database_exists(cursor,DB_NAME): 
            e_sql = 'use {}'.format(DB_NAME)
            cursor.execute(e_sql) 
            if table_exists(cursor,table_name):
                e_sql = 'drop table {}'.format(table_name)
                cursor.execute(e_sql)
                print('drop_table function: drop table {table}'.format(table_name))
            else:
                print('drop_table function: table {} is not exists'.format(table_name))
        else:
            print('drop_table function: database {} is not exists'.format(DB_NAME))
    except:
        pass
    finally:
        db.close()
if __name__ == '__main__':
    DB_NAME = 'm12345'
    table_name = 'fae'
    table_format = '''iNr int(10),x_pos double(15,9),
            y_pos double(15,9),z_pos double(15,9), valid enum('Y','N')'''
    creat_table(DB_NAME,table_name,table_format)
#    drop_database(DB_NAME)
#    DB_NAME = 'datapp'
#    table_name = 'fae1'
#    drop_table(DB_NAME,table_name)
#e_sql = '''insert {} value(1,1.2,1.3,1.4,'Y')'''.format(table_name)
#cursor.execute(e_sql)
#db.commit()
