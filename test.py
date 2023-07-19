"""
# Project:
# Author: Eddie
# Date: 
"""
from datetime import datetime
from googletrans import Translator
from pymysql import Error
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


a = translator('3 juni 2022', src='nl')
print(a)