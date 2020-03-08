import os
from azure.cognitiveservices.language.textanalytics import TextAnalyticsClient
from msrest.authentication import CognitiveServicesCredentials

class CogAPI:
    def __init__(self, _subscription_key, _endpoint):
        self.subscription_key = _subscription_key
        self.endpoint = _endpoint

    def authenticateClient(self):
        credentials = CognitiveServicesCredentials(self.subscription_key)
        text_analytics_client = TextAnalyticsClient( endpoint = self.endpoint, credentials = credentials )
        return text_analytics_client

    def getSentiment(self, Tweets):        
        client = self.authenticateClient()
        documents = list()
        idCounter = 1
        
        try:
            for tweet in Tweets:
                documents.append({"id": str(idCounter), "language": "en", "text": tweet.full_text})
                idCounter += 1

            sentimentScoreResponse = client.sentiment(documents = documents)
            ScoreList = list()
            for document in sentimentScoreResponse.documents:
                ScoreList.append(document.score)
        
            return ScoreList
        except Exception as err:
            print("Encountered exception. {}".format(err))  
                
    def getKeyPhrases(self, Tweets):        
        client = self.authenticateClient()
        documents = list()
        idCounter = 1
        
        try:
            for tweet in Tweets:
                documents.append({"id": str(idCounter), "language": "en", "text": tweet.full_text})
                idCounter += 1

            key_phrasesResponse = client.key_phrases(documents = documents)
            key_phrasesList = list()
            for document in key_phrasesResponse.documents:
                key_phrasesList.append(str(document.key_phrases))
            return key_phrasesList
        except Exception as err:
            print("Encountered exception. {}".format(err))