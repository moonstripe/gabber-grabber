from transformers import pipeline, Pipeline
import pandas as pd
import requests
import aiohttp
import asyncio
import re
import psycopg2
from dotenv import load_dotenv
import os
import ssl
import itertools
from sqlalchemy import create_engine
from datetime import datetime

# Disable SSL certificate verification (for testing/development purposes)
ssl_ctx = ssl.create_default_context()

class SentimentAnalyzerXLMRoberta:
    def __init__(self):
        model_path = "cardiffnlp/twitter-xlm-roberta-base-sentiment"
        self.sentiment_task: Pipeline = pipeline("sentiment-analysis", model=model_path, tokenizer=model_path)

    # returns a non-normalized sentiment {label: str, score: float}
    def analyze_sentiment(self, statement: str):
        return self.sentiment_task(statement)
    
    # returns a normalized sentiment (between 0 and 1 score)
    def analyze_sentiment_normal(self, statement: str) -> float:
        analysis = self.sentiment_task(statement)[0]

        if analysis['label'] == 'positive':
            return analysis['score']/2 + 0.5
        if analysis['label'] == 'negative':
            return (1-analysis['score'])/2
        if analysis['label'] =="neutral":
            return -1
        

    def sentence_split_then_analyze(self, statement: str):
        split_arr = re.split('.|\t|\n', statement)

        sum = 0
        total = 0
        for sentence in split_arr:
            sentiment = self.analyze_sentiment_normal(sentence)
            if sentiment > 0:
                sum += sentiment
                total += 1
        if total == 0:
            return -1
        else: 
            return round(sum/total, 4)

        
# SYNCHRONOUS

# class GabScraper:
#     def __init__(self):
#         self.api_base: str = "https://gab.com/api"
#         self.headers = {
#             "Cache-Control": "no-cache",
#             "User-Agent": "Python Script",
#             "Accept": "*/*",
#             "Connection": "keep-alive"
#         }

#     def search_statuses(self, status_query: str):
#         status_arr = []
#         url = f'{self.api_base}/v3/search?type=status&q={status_query}&page=1'
#         pattern = r'<(.*?)>; '

#         while url is not None:
#             resp = requests.get(url, headers=self.headers)
#             json = resp.json()

#             if 'statuses' not in json:
#                 return status_arr

#             status_arr = status_arr + json['statuses']
#             matches = re.findall(pattern, resp.headers['link'])

#             if matches:
#                 print(matches[0])   
#                 url = matches[0]
#             else:
#                 print("No match found.")
#                 return status_arr
        
#         return status_arr

#     def get_all_lists(self):
#         url = f'{self.api_base}/v2/lists'

#         resp = requests.get(url, headers=self.headers)
#         json = resp.json()

#         return json
    
#     def get_all_groups(self):
#         url = f'{self.api_base}/v1/groups'

#         resp = requests.get(url, headers=self.headers)
#         json = resp.json()

#         return json
    
#     def get_all_posts_from_group(self, group_id: int):
#         url = f'{self.api_base}/v1/timelines/{group_id}?sort_by=newest'

#         resp = requests.get(url, headers=self.headers)
#         json = resp.json()

#         return json
    
#     def get_all_posts_from_list(self, list_id: int):
#         url = f'{self.api_base}/v1/timelines/{list_id}'

#         resp = requests.get(url, headers=self.headers)
#         json = resp.json()

#         return json
    
class GabScraper:
    def __init__(self):
        self.api_base: str = "https://gab.com/api"
        self.headers = {
            "Cache-Control": "no-cache",
            "User-Agent": f"python-rest-client-{datetime.now()}",
            "Accept": "*/*",
            "Connection": "keep-alive"
        }
        self.ssl_ctx = ssl.create_default_context()
        self.ssl_ctx.check_hostname = False
        self.ssl_ctx.verify_mode = ssl.CERT_NONE

    # async def search_statuses(self, status_query: str):
    #     status_arr = []
    #     url = f'{self.api_base}/v3/search?type=status&q={status_query}&page=1'
    #     pattern = r'<(.*?)>; '

    #     async with aiohttp.ClientSession(headers=self.headers, connector=aiohttp.TCPConnector(ssl=self.ssl_ctx), timeout=aiohttp.ClientTimeout(total=60)) as session:
    #         while url is not None:
    #             async with session.get(url) as resp:
    #                 json = await resp.json()

    #                 if 'statuses' not in json:
    #                     return status_arr

    #                 status_arr += json['statuses']
    #                 matches = re.findall(pattern, resp.headers['link'])

    #                 if matches:
    #                     print(matches[0])
    #                     url = matches[0]
    #                 else:
    #                     print("No match found.")
    #                     return status_arr
        
    #     return status_arr

    async def get_all_lists(self):
        url = f'{self.api_base}/v2/lists'

        async with aiohttp.ClientSession(headers=self.headers, connector=aiohttp.TCPConnector(ssl=self.ssl_ctx), timeout=aiohttp.ClientTimeout(total=60)) as session:
            async with session.get(url) as resp:
                json = await resp.json()

                return json

    async def get_all_groups(self):
        url = f'{self.api_base}/v1/groups'

        async with aiohttp.ClientSession(headers=self.headers, connector=aiohttp.TCPConnector(ssl=self.ssl_ctx), timeout=aiohttp.ClientTimeout(total=60)) as session:
            async with session.get(url) as resp:
                # print(await resp.text())
                json = await resp.json()

                return json
    
    async def get_all_posts_from_group(self, group_id: str):
        page = 1
        url = f'{self.api_base}/v1/timelines/group/{group_id}?page={page}&sort_by=newest'
        all_posts = []

        async with aiohttp.ClientSession(headers=self.headers, connector=aiohttp.TCPConnector(ssl=self.ssl_ctx), timeout=aiohttp.ClientTimeout(total=60)) as session:
            while url is not None:
                async with session.get(url) as resp:
                    try:
                        json = await resp.json()
                        all_posts += json
                        page += 1

                        url = f'{self.api_base}/v1/timelines/group/{group_id}?page={page}&sort_by=newest'
                    except:
                        return all_posts

    async def get_all_posts_from_list(self, list_id: str):
        url = f'{self.api_base}/v1/timelines/list/{list_id}'

        async with aiohttp.ClientSession(headers=self.headers, connector=aiohttp.TCPConnector(ssl=self.ssl_ctx), timeout=aiohttp.ClientTimeout(total=60)) as session:
            async with session.get(url) as resp:
                json = await resp.json()
                return json
            
def pull_useful_columns(response, analyzer: SentimentAnalyzerXLMRoberta):
    return {
        "id": response["id"],
        "url": response["url"],
        "author_username": response["account"]["username"],
        "content": response["content"],
        "date": response["created_at"],
        "sentiment": analyzer.sentence_split_then_analyze(response['content'])
    }


async def main():
    load_dotenv()
    # PostgreSQL connection details
    db_user = os.getenv("DB_USER") # postgres
    db_password = os.getenv("DB_PASS")
    db_host = os.getenv("DB_HOST") # 127.0.0.1
    db_port = os.getenv("DB_PORT") # 5432
    db_name = os.getenv("DB_NAME")

    # Create a SQLAlchemy engine
    engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

    # Serialize DataFrame to PostgreSQL
    table_name = 'posts'  # Name of the table you want to create or append to
    
    # Initialize scraper object
    scraper = GabScraper()

    # Pull all groups
    all_groups = await scraper.get_all_groups()

    # Asynchronously complete response tasks
    group_post_tasks = [scraper.get_all_posts_from_group(group['id']) for group in all_groups]
    group_post_responses = await asyncio.gather(*group_post_tasks)

    all_responses = list(itertools.chain(*group_post_responses))

    print("successfully pulled all posts")

    # Initialize Sentiment Analyzer
    sentiment = SentimentAnalyzerXLMRoberta()

    # Collect columns we care about
    useful_responses = [pull_useful_columns(response=response, analyzer=sentiment) for response in all_responses]

    # Pull into pandas
    df = pd.DataFrame(useful_responses)

    # Serialize into PostgreSQL
    df.to_sql(table_name, engine, if_exists='replace', index=False)

    # Close the engine
    engine.dispose()

if __name__ == "__main__":
    asyncio.run(main())


