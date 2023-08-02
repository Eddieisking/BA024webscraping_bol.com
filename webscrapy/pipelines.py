# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import openpyxl
import pymysql
import re
import googletrans
from datetime import datetime
from googletrans import Translator
from pymysql import Error

"""
    review_id = scrapy.Field()
    product_name = scrapy.Field()
    customer_name = scrapy.Field()
    customer_rating = scrapy.Field()
    customer_date = scrapy.Field()
    customer_review = scrapy.Field()
    customer_support = scrapy.Field()
"""

# Pipeline for Excel
class ExcelPipeline:

    def __init__(self):
        self.wb = openpyxl.Workbook()
        self.ws = self.wb.active
        self.ws.title = 'customer reviews'
        self.ws.append(('review_id','product_name','customer_name', 'customer_rating', 'customer_date', 'customer_review', 'customer_support', 'customer_disagree'))

    def open_spider(self, spider):
        pass

    def close_spider(self, spider):
        self.wb.save('bol.xlsx')

    def process_item(self, item, spider):
        review_id = item.get('review_id', '')
        product_name = item.get('product_name', '')
        customer_name = item.get('customer_name', '')
        customer_rating = item.get('customer_rating', '')
        customer_date = item.get('customer_date', '')
        customer_review = item.get('customer_review', '')
        customer_support = item.get('customer_support', '')
        customer_disagree = item.get('customer_disagree', '')

        self.ws.append((review_id, product_name, customer_name, customer_rating, customer_date, customer_review, customer_support, customer_disagree))
        return item

"""
    review_id = scrapy.Field()
    product_name = scrapy.Field()
    customer_name = scrapy.Field()
    customer_rating = scrapy.Field()
    customer_date = scrapy.Field()
    customer_review = scrapy.Field()
    customer_support = scrapy.Field()
"""
# Pipeline for sql
def remove_unappealing_characters(text):
    # Remove emojis
    text = text.encode('ascii', 'ignore').decode('ascii')

    # Remove non-printable characters
    text = re.sub(r'[^\x20-\x7E]', '', text)

    return text

def translator(text: str, src: str):

    # print(googletrans.LANGUAGES)

    translator = Translator()
    result = translator.translate(text, src=src, dest='en')

    return result.text

def extract_translate_month(date_str, src):
    translator = Translator()
    month_italian = date_str.split()[1]
    translated_month = translator.translate(month_italian, src=src, dest='en').text
    translated_date_str = date_str.replace(month_italian, translated_month)

    return translated_date_str

def date(date: str):
    date_object = datetime.strptime(date, '%d %B %Y')
    return date_object.date()

def extract_review_code(string):
    return string.split("review-")[1]

def extract_rating_number(string):
    rating = re.search(r"\d+", string).group()
    return int(rating)

class DatabasePipeline:

    def __init__(self):
        self.conn = pymysql.connect(user="fqmm26", password="boston27", host="myeusql.dur.ac.uk", database="Pfqmm26_BA024")
        self.cursor = self.conn.cursor()

    def close_spider(self, spider):
        self.conn.commit()
        self.cursor.close()
        self.conn.close()

    def process_item(self, item, spider):
        try:
            self.cursor.execute("SELECT 1")  # Execute a simple query to check if the connection is alive
        except Error as e:
            print(f"Error: {e}")
            self.reconnect()
        # review_id = item.get('review_id', '')
        # product_name = item.get('product_name', '')
        # customer_name = item.get('customer_name', '')
        # customer_rating = item.get('customer_rating', '')
        # customer_date = item.get('customer_date', '')
        # customer_review = item.get('customer_review', '')
        # customer_support = item.get('customer_support', '')
        # customer_disagree = item.get('customer_disagree', '')
        review_id = extract_review_code(item.get('review_id', ''))
        product_name = item.get('product_name', '')
        customer_name = item.get('customer_name', '')
        customer_rating = extract_rating_number(item.get('customer_rating', ''))
        customer_date = date(extract_translate_month(item.get('customer_date', ''), src='nl'))
        # Remove unloaded chars and cut
        # customer_review_original = item.get('customer_review', '')
        # customer_review = remove_unappealing_characters(' '.join(customer_review_original))
        customer_review = item.get('customer_review', '')
        customer_support = item.get('customer_support', '')
        customer_disagree = item.get('customer_disagree', '')
        product_website = item.get('product_website', '')
        product_brand = item.get('product_brand', '')
        product_model = item.get('product_model', '')
        product_type = item.get('product_type', '')

        # Translate language into english
        product_name_en = translator(product_name, src='nl')
        customer_review_en = translator(customer_review, src='nl')

        try:
            self.cursor.execute(
                "INSERT INTO bol_nl (review_id, product_name, customer_name, customer_rating, customer_date, "
                "customer_review, customer_support, customer_disagree, product_name_en, customer_review_en, product_website, product_type, product_brand, product_model) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (review_id, product_name, customer_name, customer_rating, customer_date, customer_review,
                 customer_support, customer_disagree, product_name_en, customer_review_en, product_website, product_type, product_brand, product_model)
            )
            self.conn.commit()
        except Error as e:
            print(f"Error inserting item into database: {e}")

        return item

    def reconnect(self):
        try:
            self.conn.ping(reconnect=True)  # Ping the server to reconnect
            print("Reconnected to the database.")
        except Error as e:
            print(f"Error reconnecting to the database: {e}")

