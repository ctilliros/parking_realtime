import psycopg2
import time
from timeloop import Timeloop
from datetime import timedelta
from IPython.display import clear_output
import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import ConnectionError
from datetime import datetime
# conn = psycopg2.connect(host="localhost", database="testing", user="postgres", password="9664241907")

conn = psycopg2.connect(host="localhost", port=5433, database="postgres", user="admin", password="secret")
if conn:
    print('Success!')
else:
    print('An error has occurred.')
cursor = conn.cursor()

def create_tables(conn):    
    sql_create = 'CREATE TABLE IF NOT EXISTS parking_info(parking_id SERIAL NOT NULL, ' \
                 'parking_name character varying(100) COLLATE pg_catalog."default",' \
                 ' parking_code integer NOT NULL,' \
                 'latitude double precision NOT NULL,' \
                 'longitude double precision NOT NULL,' \
                 'CONSTRAINT "parking_info_pkey" PRIMARY KEY (parking_id))'
    cursor.execute(sql_create, )
    conn.commit()
    sql_create = 'CREATE TABLE IF NOT EXISTS parking_updatetime(parking_updatetime_id SERIAL NOT NULL,' \
                 'date date NOT NULL,' \
                 '"time" time without time zone NOT NULL,' \
                 'datetime timestamp without time zone,' \
                 'CONSTRAINT "parking_updatetime_pkey" PRIMARY KEY (parking_updatetime_id))'
    cursor.execute(sql_create, )
    conn.commit()

    sql_create = 'CREATE TABLE IF NOT EXISTS parking_freespaces(parking_freespaces_id SERIAL NOT NULL,' \
                 'value integer NOT NULL,' \
                 'id_parkingFK integer,' \
                 'id_updatetimeFK integer,' \
                 'CONSTRAINT "parking_freespaces_pkey" PRIMARY KEY (parking_freespaces_id),' \
                 'FOREIGN KEY (id_parkingFK) REFERENCES parking_info (parking_id),' \
                 'FOREIGN KEY (id_updatetimeFK) REFERENCES parking_updatetime (parking_updatetime_id))'
    cursor.execute(sql_create, )
    conn.commit()

def parse_date(date,time,parking_date):
    # print(date,time,parking_date)    
    sql_date = 'select datetime, date from parking_updatetime where datetime = %s;'
    cursor.execute(sql_date, (parking_date,))
    conn.commit()
    row_date_count = cursor.rowcount
    if row_date_count == 0:        
        sql_insert_parking_update = 'insert into parking_updatetime(date, time, datetime) values(%s,%s,%s)'
        cursor.execute(sql_insert_parking_update, (date,time, parking_date))
        conn.commit()

def parse_value(value,parking_name):
    sql_date = 'select max(parking_updatetime_id) from parking_updatetime'
    cursor.execute(sql_date,)
    update_id = cursor.fetchone()[0]
    sql_parking_info = 'select parking_id from parking_info where parking_name = %s '
    cursor.execute(sql_parking_info,(parking_name,))
    parking_name_id = cursor.fetchone()[0]
    sql_check_value = 'select * from parking_freespaces where id_updatetimefk = %s and id_parkingfk = %s'
    cursor.execute(sql_check_value,(update_id,parking_name_id))
    value_count = cursor.rowcount
    if value_count == 0:
        sql_insert_value = 'insert into parking_freespaces(value,id_parkingfk,id_updatetimefk) values(%s,%s,%s)'
        cursor.execute(sql_insert_value, (value,parking_name_id,update_id))
        conn.commit()

tl = Timeloop()
@tl.job(interval=timedelta(seconds=1))
def sample_job_every_1000s():    
    # clear_output(wait=False)
    try:
        url ='http://www.nicosia.org.cy/el-GR/rss/parkingspaces/'
        import ssl
        if hasattr(ssl, '_create_unverified_context'):
            ssl._create_default_https_context = ssl._create_unverified_context
        import feedparser
        feed = feedparser.parse(url)        
        create_tables(conn)
        i = 0
        for post in feed.entries:            
            i+=1
            parking_id = post.id[-1:]
            parking_name = post.title
            parking_update_time = post.updatedon
            parking_date = datetime.strptime(parking_update_time, '%a, %d %b %Y %H:%M:%S')
            date = datetime.strftime(parking_date, '%a, %d %b %Y')
            time = datetime.strftime(parking_date, '%H:%M:%S')
            latitude = post.geolocation.split(',')[0]
            longitude = post.geolocation.split(',')[1]
            value = post.summary
            print("ALKAKJSDjk")
            sql_create_first = 'select * from parking_info'
            print("asfds")
            cursor.execute(sql_create_first,)
            row_count = cursor.rowcount
            if row_count < i:
                sql_insert_parking_info = 'insert into parking_info(parking_name,parking_code,latitude,longitude) values(%s,%s,%s,%s)'
                cursor.execute(sql_insert_parking_info, (parking_name, parking_id, latitude, longitude))
                conn.commit()
                parse_date(date,time,parking_date)
                parse_value(value,parking_name)
            else:
                parse_date(date,time,parking_date)
                parse_value(value,parking_name)

    except ConnectionError as ce:
        print(ce)


if __name__ == "__main__":
    tl.start(block=True)
