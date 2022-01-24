import threading
from typing import List, Dict
import sys
import json

from bs4 import BeautifulSoup

from src.processors.processor import Processor
from src.processors.types import Record


class RottenTomatoes:
    def __init__(self, tomatometer_score, audience_score):
        self.tomatometer_score = tomatometer_score
        self.audience_score = audience_score
        self.num_tomatometer = 0
        self.num_audience = 0

    def set_num_audience(self,n):
        self.num_audience = n

    def set_num_tomatometer(self,n):
        self.num_tomatometer = n


def getNumReviews(soup):
    critic = soup.find_all("small", class_="mop-ratings-wrap__text--small")
    audience = soup.find_all("strong", class_="mop-ratings-wrap__text--small")
    if critic:
        critic = critic[0].text.replace("\n", '').strip()
    else:
        critic = soup.find_all("a", class_='scoreboard__link scoreboard__link--tomatometer')
        critic = critic[0].text
    if len(audience) > 1:
        audience = audience[1].text.replace("Verified Ratings: ", '')
    else:
        audience = soup.find_all("a", class_='scoreboard__link scoreboard__link--audience')
        if audience:
            audience = audience[0].text
        else:
            audience = 0

    return [int(critic), int(audience)]


def getScoreOld(soup) -> RottenTomatoes:
    critic = soup.find('span', {'class': "meter-value superPageFontColor"})
    audience = soup.find('div', {'class': "audience-score meter"}).find('span', {'class': "superPageFontColor"})
    return  RottenTomatoes(critic.text, audience.text)


def getScoreNew(soup) -> RottenTomatoes:
    temp = soup.find_all('div', class_='mop-ratings-wrap__half')
    try:
        critic = temp[0].text.strip().replace('\n', '').split(' ')[0] #2020 scraper
        if len(temp) > 1:
            audience = temp[1].text.strip().replace('\n', '').split(' ')[0]
        else:
            audience = "None"
    except:
        scores = soup.find("score-board") #2021 scraper
        return RottenTomatoes(scores["tomatometerscore"], scores["audiencescore"])
    rt = RottenTomatoes(critic, audience)

    num_critic, num_audience = getNumReviews(soup)

    rt.set_num_audience(num_audience)
    rt.set_num_tomatometer(num_critic)

    return rt




def get_metadata(html_payload: str) -> RottenTomatoes:
    soup = BeautifulSoup(html_payload, 'lxml')

    try:
       return getScoreOld(soup)
    except:
        try:
            return getScoreNew(soup)
        except:
            return None


class RottenTomatoesProcessor(Processor):
    def __init__(self, results: List[Dict], mutex: threading.Lock):
        self.results = results
        self.mutex = mutex
        super().__init__()

    def process(self, record: Record):
        has_lock  = False
        try:
            rt = get_metadata(record.content)
            self.mutex.acquire()
            has_lock  = True
            self.results.append({
                "uri": record.uri,
                "ts": record.ts,
                "criticScore": rt.tomatometer_score,
                "criticNum": rt.num_tomatometer,
                "audienceScore": rt.audience_score,
                "audienceNum": rt.num_audience

            })
        except Exception as e:
            self.results.append({'error': f'Error processing: {record.uri}', 'reason': str(e)})
        finally:
            if has_lock:
                self.mutex.release()

