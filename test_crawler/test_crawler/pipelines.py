# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import scrapy
import pymongo
import json
import csv
import mysql.connector
import psycopg2
# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem

class MongoDBBongDa24HPipeline:
    def __init__(self):
        self.client = pymongo.MongoClient("mongodb://localhost:27017/")
        self.db = self.client["BongDa24HDB"]
        self.collection = self.db["dbbongda24h"]
        pass

    def process_item(self, item, spider):
        try:
            self.collection.insert_one(dict(item))
            return item
        except Exception as e:
            raise DropItem(f"Error inserting item: {e}")
    
    def close_spider(self, spider):
        self.client.close()  # Đóng kết nối MongoDB

class JsonDBBongDa24HPipeline:
    def __init__(self):
        self.data = []

    def process_item(self, item, spider):
        self.data.append(dict(item))
        return item

    def open_spider(self, spider):
        self.file = open('D:\\test_crawler\\test_crawler\\data\\json\\jsondata.json', 'w', encoding='utf-8')

    def close_spider(self, spider):
        json.dump(self.data, self.file, ensure_ascii=False, indent=4)
        self.file.close()

    
class CSVBongDa24HPipeline:
    def __init__(self):
        self.file = open("D:\\test_crawler\\test_crawler\data\\csv\\football.csv", "a", newline="", encoding="utf-8")
        self.csv_writer = csv.writer(self.file, delimiter="$")
        self.csv_writer.writerow(["days", "name_match","result", "match_rounds"])  # Ghi header chỉ 1 lần

    def process_item(self, item, spider):
        days = item.get("days", "")
        name_match = item.get("name_match", "")
        result = item.get("result", "")
        match_rounds = item.get("match_rounds", "")
        self.csv_writer.writerow([days, name_match, result, match_rounds])  # Chuyển thành list
        return item

    def close_spider(self, spider):
        self.file.close()

class MySQLBongDa24HPipline:
    def __init__(self):
        self.connect = mysql.connector.connect(
            host = "localhost",
            user = "root",
            password = "12345678",
            database = "bongda24hdb"
        )
        self.cursor = self.connect.cursor()
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS bongda24h_crawler(
            id INT NOT NULL AUTO_INCREMENT,
            days TEXT,
            name_match TEXT,
            result TEXT,
            match_rounds TEXT,
            PRIMARY KEY(id)                
        )
        """)
    
    def process_item(self, item, spider):
        self.cursor.execute('''
            INSERT INTO bongda24h_crawler(days, name_match, result, match_rounds)
            VALUES(%s, %s, %s, %s)
                            ''',(
            item['days'],
            item['name_match'],
            item['result'],
            item['match_rounds']                    
                            ))
        self.connect.commit()
        return item
    
    def close_spider(self, spider):
        self.cursor.close()
        self.connect.close()

class TxtBongDa24HPipline:
    def open_spider(self, spider):
        self.file = open("D:\\test_crawler\\test_crawler\\data\\txt\\football.txt", "w", encoding="utf-8")

    def process_item(self, item, spider):
        days = item.get("days", "Không rõ ngày")
        name_match = item.get("name_match", "Không rõ trận đấu")
        result = item.get("result", "Chưa có kết quả")
        match_rounds = item.get("match_rounds", "Không rõ vòng")

        line = f"Ngày: {days} | Trận đấu: {name_match} | Kết quả: {result} | Vòng đấu: {match_rounds}\n"

        self.file.write(line)
        return item
    def close_spider(self, spider):
        self.file.close()

class PostgresBongDaPipeline:
    def __init__(self):
        self.connect = psycopg2.connect(
            host='localhost',
            user='postgres',
            password='12345678',
            database='dbbongda24h'
        )
        self.cursor = self.connect.cursor()
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS bongda24h (
            id SERIAL PRIMARY KEY,
            days TEXT,
            name_match TEXT,
            result TEXT,
            match_rounds TEXT
        )
        """)
        self.connect.commit()

    def process_item(self, item, spider):
        self.cursor.execute('''
            INSERT INTO bongda24h (days, name_match, result, match_rounds)
            VALUES (%s, %s, %s, %s)
        ''', (
            item.get('days', ''),
            item.get('name_match', ''),
            item.get('result', ''),
            item.get('match_rounds', '')
        ))
        self.connect.commit()
        return item

    def close_spider(self, spider):
        self.cursor.close()
        self.connect.close()