from googlesearch import search
from queue import PriorityQueue
from datetime import date, datetime
from urllib.parse import urlparse, urljoin
from urllib.request import urlopen
from bs4 import BeautifulSoup
import threading
import csv


class Log:
    def __init__(self):
        self.fileName = "crawl_log_" + \
            str(date.today().strftime("%d-%m-%Y"))+".csv"
        self.fields = ['url', 'size', 'depth',
                       'score', 'time_of_crawl', 'thread_name']
        self.lock = threading.Lock()
        self.logFileWriter, self.logFile = self.createLog()
        self.totalLogWrites = 0

    def createLog(self):
        logFile = open(self.fileName, 'w', newline='')
        filewriter = csv.writer(logFile, delimiter=',')
        filewriter.writerow(self.fields)
        return filewriter, logFile

    def writeLog(self, row):
        self.totalLogWrites += 1
        self.lock.acquire()
        self.logFileWriter.writerow(row)
        self.lock.release()

    def closeLog(self):
        self.logFile.close()


class Crawler:
    def __init__(self, maxLinks):
        self.maxLinks = maxLinks
        self.priorityQueue = PriorityQueue()
        self.visitedUrlMap = {}
        self.visitedDomainsNoveltyScore = {}
        self.importanceScore = {}
        self.lock = threading.Lock()
        self.totalUrls = 0
        self.totalLogsCalls = 0

    def addUrlsToCrawl(self, urls, newUrlsFlag):
        for url in urls:
            if url not in self.visitedUrlMap:
                self.visitedUrlMap[url] = 1
                if newUrlsFlag:
                    self.totalUrls += 1
                    parsedUrl = urlparse(url)
                    domain = parsedUrl.netloc
                    if domain not in self.visitedDomainsNoveltyScore:
                        self.visitedDomainsNoveltyScore[domain] = (
                            100, datetime.now())
                    else:
                        noveltyScore = self.visitedDomainsNoveltyScore[domain][0]
                        self.visitedDomainsNoveltyScore[domain] = (
                            noveltyScore - 5, datetime.now())
                    if url not in self.importanceScore:
                        self.importanceScore[url] = 10
                    else:
                        self.importanceScore[url] += 10

                if newUrlsFlag is False or self.totalUrls <= self.maxLinks:
                    self.priorityQueue.put(
                        (self.calculateScore(url) * -1, (url, datetime.now())))

    def normalizeUrl(self, base_url, link):
        base_url = urljoin(base_url, '/')
        if link is None or link == '#' or link.startswith('#'):
            return None
        if '#' in link:
            link = link.split('#', 1)[0]
        if link.startswith('//'):
            return 'http' + link
        if link.startswith('/'):
            return base_url[:-1] + link if base_url[-1] == '/' else base_url + link

        return link

    def calculateScore(self, url):
        noveltyScore = 100
        importanceScore = 0
        parsedUrl = urlparse(url)
        domain = parsedUrl.netloc
        if domain in self.visitedDomainsNoveltyScore:
            noveltyScore = self.visitedDomainsNoveltyScore[domain][0]
        else:
            self.visitedDomainsNoveltyScore[domain] = (100, datetime.now())

        if url in self.importanceScore:
            importanceScore = self.importanceScore[url]
        else:
            importanceScore = 10
            self.importanceScore[url] = 10

        return 0.5 * noveltyScore + importanceScore

    def crawlWeb(self, log):
        try:
            while not self.priorityQueue.empty():
                self.lock.acquire()
                url = ""
                while True:
                    score, (newUrl, timeStamp) = self.priorityQueue.get_nowait()
                    parsedUrl = urlparse(newUrl)
                    domain = parsedUrl.netloc
                    baseUrl = parsedUrl.scheme + '://' + domain
                    if newUrl == url or timeStamp > self.visitedDomainsNoveltyScore[domain][1]:
                        break

                    del self.visitedUrlMap[newUrl]
                    self.addUrlsToCrawl([newUrl], False)
                    url = newUrl

                row = [newUrl, '10', '1', score*-1, datetime.now().strftime(
                    "%d-%m-%Y %H:%M:%S"), threading.currentThread().getName()]
                self.lock.release()
                log.writeLog(row)
                self.totalLogsCalls += 1

                if self.totalUrls > self.maxLinks:
                    continue
                try:
                    page = urlopen(newUrl)
                    mimeType = page.info().get_content_maintype()
                    if mimeType != 'text' or page.getcode() != 200:
                        continue
                    htmlContent = page.read().decode('utf8')
                except Exception as e:
                    print("Exception occured during opening a page =", e)
                    print("url after exception ===", newUrl)
                    continue

                soup = BeautifulSoup(htmlContent, 'html.parser')
                newUrls = []
                for a in soup("a"):
                    if self.totalUrls > self.maxLinks:
                        break
                    try:
                        fullUrl = self.normalizeUrl(baseUrl, a['href'])

                        if fullUrl is not None:
                            if fullUrl not in self.visitedUrlMap:
                                newUrls.append(fullUrl)
                    except:
                        continue
                self.lock.acquire()
                self.addUrlsToCrawl(newUrls, True)
                self.lock.release()
        except Exception as e:
            print("Exception occured =", e)
            self.lock.release()
            return


if __name__ == '__main__':
    searchResults = search("rubique", stop=10)
    crawler = Crawler(990)
    log = Log()
    crawler.addUrlsToCrawl(searchResults, False)

    threads = []
    start = datetime.now()
    for i in range(10):
        thread = threading.Thread(
            target=crawler.crawlWeb, name="name"+str(i), args=(log,))
        threads.append(thread)
        thread.start()

    # wait for all threads to finish
    for thread in threads:
        thread.join()

    print("total time =", datetime.now() - start)
    print("totalUrls =", crawler.totalUrls)
    print("priorityQueue size =", crawler.priorityQueue.qsize())
    print("totalLogWrites calls ===", log.totalLogWrites,
          " self.totalLogsCalls ====", crawler.totalLogsCalls)
