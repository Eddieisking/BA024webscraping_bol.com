"""
Project: Web scraping for customer reviews
Author: Hào Cui
Date: 07/04/2023
"""
import json
import re

import scrapy
from scrapy import Request

from webscrapy.items import WebscrapyItem


class SpiderSpider(scrapy.Spider):
    name = "spider"
    allowed_domains = ["www.bol.com", "api.bazaarvoice.com"]
    headers = {} 

    def start_requests(self):
        # keywords = ['Stanley', 'Black+Decker', 'Craftsman', 'Porter-Cable', 'Bostitch', 'Facom', 'MAC Tools', 'Vidmar', 'Lista', 'Irwin Tools', 'Lenox', 'Proto', 'CribMaster', 'Powers Fasteners', 'cub-cadet', 'hustler', 'troy-bilt', 'rover', 'BigDog Mower', 'MTD']
        # company = 'Stanley Black and Decker'
        exist_keywords = ['dewalt', 'Stanley', 'Black+Decker', 'Bostitch', 'Facom', 'Powers Fasteners', 'cub-cadet']

        # from search words to generate product_urls
        for keyword in exist_keywords:
            push_key = {'keyword': keyword}
            search_url = f'https://www.bol.com/nl/nl/s/?searchtext={keyword}'

            yield Request(
                url=search_url,
                callback=self.parse,
                cb_kwargs=push_key,
            )

    def parse(self, response, **kwargs):
        # Extract the pages of product_urls
        page = response.xpath('//*[@id="js_list_view"]//p[@class="total-results js_total_results"]/text()')[0].extract()

        # Remove any non-digit characters from the string
        number_string = ''.join(filter(str.isdigit, page))

        # Convert the extracted string into an integer
        page_number = int(number_string)
        pages = (page_number // 24) + 1

        # Based on pages to build product_urls
        keyword = kwargs['keyword']
        product_urls = [f'https://www.bol.com/nl/nl/s/?page={page}&searchtext={keyword}&view=list' for page
                        in range(1, pages + 1)]  # pages + 1 
        
        for product_url in product_urls:
            yield Request(url=product_url, callback=self.product_parse, meta={'product_brand': keyword})

    def product_parse(self, response: Request, **kwargs):
        product_list = response.xpath('//*[@id="js_items_content"]/li')
        product_brand = response.meta['product_brand']

        for product in product_list:
            product_href = product.xpath('.//wsp-analytics-tracking-event/a[@data-test="product-title"]/@href')[0].extract()
            product_detailed_url = f'https://www.bol.com{product_href}'
            product_id = product.xpath('./@data-id')[0].extract()
            yield Request(url=product_detailed_url, callback=self.product_detailed_parse, meta={'product_id': product_id, 'product_brand':product_brand})

    def product_detailed_parse(self, response, **kwargs):
        # Product_id
        product_id = response.meta['product_id']
        product_brand = response.meta['product_brand']
        product_detail = response.xpath('//div[@class="js_specifications_content js_show-more-content"]//div[@class="specs"]//div[@class="specs__row"]')
        product_model = 'N/A'
        product_type = 'N/A'

        for product in product_detail:
            attr = product.xpath('./dt[@class="specs__title"]/text()')[0].extract().strip()
            value = product.xpath('./dd[@class="specs__value"]/text()').extract_first()
            if not value:
                value = product.xpath('./dd[@class="specs__value"]/a/text()').extract_first()
            # If needed, strip the whitespace from the value
            if value:
                value = value.strip()
            if attr == "Type boormachine hulpstuk":
                product_type = value if value else 'N/A'
            elif attr == 'MPN (Manufacturer Part Number)':
                product_model = value if value else 'N/A'

        # Total reviews number
        total_reviews = response.xpath('//div[@class="reviews-summary-flex"]/div[@data-test="total-reviews"]/text()').extract()
        if total_reviews:
            total_number = re.search(r'\d+', total_reviews[0]).group()
        else:
            total_number = None
            
        # Product_name
        product_name = response.xpath('//*[@id="product_title"]/h1/span[@data-test="title"]/text()')[0].extract()

        # Product reviews url
        if total_number:
            customer_review_url = f'https://www.bol.com/nl/rnwy/productPage/reviews?productId={product_id}&offset=0&limit={total_number}&loadMore=true'
            yield Request(url=customer_review_url, callback=self.review_parse, meta={'product_name': product_name, 'product_model':product_model, 'product_brand':product_brand, 'product_type':product_type})

    def review_parse(self, response: Request, **kwargs):
        product_name = response.meta['product_name']
        product_brand = response.meta['product_brand']
        product_model = response.meta['product_model']
        product_type = response.meta['product_type']
        review_list = response.xpath('//li[@class="review js-review"]')

        for review in review_list:
            item = WebscrapyItem()
            item['review_id'] = review.xpath('./@id')[0].extract()
            item['product_website'] = 'bol_nl'
            item['product_type'] = product_type
            item['product_name'] = product_name
            item['product_brand'] = product_brand
            item['product_model'] = product_model
            item['customer_name'] = review.xpath('.//li[@data-test="review-author-name"]/text()')[0].extract()
            item['customer_rating'] = review.xpath('.//div[@class="star-rating"]/@aria-label')[0].extract()
            item['customer_date'] = review.xpath('.//li[@data-test="review-author-date"]/text()')[0].extract()
            item['customer_review'] = review.xpath('.//p[@data-test="review-body"]/text()')[0].extract()
            item['customer_support'] = review.xpath('.//div[@data-test="review-feedback-positive"]/text()')[0].extract()
            item['customer_disagree'] = review.xpath('.//div[@data-test="review-feedback-negative"]/text()')[0].extract()

            yield item



