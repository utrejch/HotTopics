import storm
from afinn import Afinn

class SentimentBolt(storm.BasicBolt):

    def __init__(self):
        self.afinn = Afinn(language='da', emoticons=True)

    def process(self, tup):
        #storm.logInfo(tup.values[0])
        score = self.afinn.score(tup.values[0])
        #storm.logInfo(str(score))
        storm.emit([str(score)])

if __name__ == "__main__":
    SentimentBolt().run()
