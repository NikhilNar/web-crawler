from googlesearch import search
from queue import PriorityQueue
from datetime import date, datetime
from urllib.parse import urlparse, urljoin
from urllib.request import urlopen
from urllib.error import HTTPError, URLError
from bs4 import BeautifulSoup
import threading
import urllib.robotparser as robotparser
import csv


class Log:
    def __init__(self):
        self.fileName = "crawl_log_" + \
            str(date.today().strftime("%d-%m-%Y"))+".csv"
        self.fields = ['url', 'size', 'depth',
                       'score', 'status_code', 'time_of_crawl', 'allowed_to_crawl', 'thread_name']
        self.lock = threading.Lock()
        self.logFileWriter, self.logFile = self.createLog()
        self.totalLogWrites = 0
        self.totalLogWriteErrors = 0

    def createLog(self):
        logFile = open(self.fileName, 'w', newline='')
        filewriter = csv.writer(logFile, delimiter=',')
        filewriter.writerow(self.fields)
        return filewriter, logFile

    def writeLog(self, row, maxWrites):
        self.lock.acquire()
        try:
            if self.totalLogWrites >= maxWrites:
                self.lock.release()
                return
            self.logFileWriter.writerow(row)
            self.totalLogWrites += 1
            print("total log writes =", self.totalLogWrites)
        except Exception as e:
            print("error occured in writing logs =", e)
            self.totalLogWriteErrors += 1
            self.lock.release()
            return

        self.lock.release()

    def closeLog(self):
        self.logFile.close()


class Crawler:
    def __init__(self, maxLinks):
        self.maxLinks = maxLinks
        self.priorityQueue = PriorityQueue()
        self.visitedUrlMap = {}
        self.robotAllowed = {}
        self.visitedDomainsNoveltyScore = {}
        self.importanceScore = {}
        self.totalUrlsParsed = 0
        self.lock = threading.Lock()
        self.totalUrls = 0
        self.totalLogsCalls = 0

    def addUrlsToCrawl(self, urls, newUrlsFlag, depth, log):
        try:
            for (url, robotAllowed) in urls:
                if url not in self.visitedUrlMap:
                    self.visitedUrlMap[url] = 1
                    if newUrlsFlag:
                        if self.totalUrls >= self.maxLinks:
                            return
                        self.totalUrls += 1
                        print("self.totalUrls ===============", self.totalUrls)
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
                            (self.calculateScore(url) * -1, (url, datetime.now(), depth, robotAllowed)))
                else:
                    self.totalUrlsParsed -= 1
                    print("same URL occured==========")
        except:
            print("exception occured while adding URLs")
        print("self.priorityQueue size ===", self.priorityQueue.qsize())

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

    def checkRobotsSafe(self, url):
        print("checkRobotsSafe called===================")
        start = datetime.now()
        robotParser = robotparser.RobotFileParser()
        if url in self.robotAllowed:
            return self.robotAllowed[url]

        parsedUrl = urlparse(url)
        robotTxtPath = "{}://{}/robots.txt".format(
            parsedUrl.scheme, parsedUrl.netloc)

        robotParser.set_url(robotTxtPath)
        robotParser.read()
        isAllowed = robotParser.can_fetch("*", url)
        self.robotAllowed[url] = isAllowed
        print("checkRobotSafe Total Time =============================",
              datetime.now() - start)
        return isAllowed

    def crawlWeb(self, log):
        try:
            while not self.priorityQueue.empty():
                self.lock.acquire()
                if log.totalLogWrites >= self.maxLinks + 10:
                    self.lock.release()
                    break

                url = ""
                count = 0
                while True:
                    score, (newUrl, timeStamp,
                            depth, robotAllowed) = self.priorityQueue.get_nowait()

                    count += 1
                    parsedUrl = urlparse(newUrl)
                    domain = parsedUrl.netloc
                    baseUrl = parsedUrl.scheme + '://' + domain
                    if newUrl == url or timeStamp >= self.visitedDomainsNoveltyScore[domain][1]:
                        break

                    print("count =============", count)

                    del self.visitedUrlMap[newUrl]
                    self.addUrlsToCrawl(
                        [(newUrl, robotAllowed)], False, depth, log)
                    url = newUrl

                self.lock.release()
                try:
                    page = urlopen(newUrl, timeout=4)
                    mimeType = page.info().get_content_maintype()
                    statusCode = page.getcode()

                    if statusCode == 200:
                        htmlContent = page.read().decode('utf8')
                        pageSize = len(htmlContent)
                    else:
                        pageSize = 0

                    row = [newUrl, pageSize, depth, score*-1, statusCode, datetime.now().strftime(
                        "%d-%m-%Y %H:%M:%S"), robotAllowed, threading.currentThread().getName()]
                    log.writeLog(row, self.maxLinks + 10)

                    if mimeType != 'text' or statusCode != 200:
                        continue

                except HTTPError as e:
                    print("Exception occured during opening a page =", e)
                    print("url after exception ===", newUrl)

                    statusCode = e.code
                    print(" statusCode  after exception ===",  statusCode)
                    row = [newUrl, 0, depth, score*-1, statusCode, datetime.now().strftime(
                        "%d-%m-%Y %H:%M:%S"), robotAllowed, threading.currentThread().getName()]
                    log.writeLog(row, self.maxLinks + 10)
                    continue
                except URLError as e:
                    print("URL ERROR OCCURRED ========")
                    row = [newUrl, 0, depth, score*-1, -1, datetime.now().strftime(
                        "%d-%m-%Y %H:%M:%S"), robotAllowed, threading.currentThread().getName()]
                    log.writeLog(row, self.maxLinks + 10)
                    continue
                except Exception as e:
                    print("Exception OCCURRED ========")
                    row = [newUrl, 0, depth, score*-1, -1, datetime.now().strftime(
                        "%d-%m-%Y %H:%M:%S"), robotAllowed, threading.currentThread().getName()]
                    log.writeLog(row, self.maxLinks + 10)
                    continue

                self.lock.acquire()
                if self.totalUrls >= 990 or not robotAllowed:
                    print("self.totalUrls ===============", self.totalUrls)
                    self.lock.release()
                    continue
                self.lock.release()

                soup = BeautifulSoup(htmlContent, 'html.parser')
                newUrls = []
                anchorTags = soup("a")
                print("anchorTags =====", len(anchorTags))
                for a in anchorTags:
                    self.lock.acquire()
                    if self.totalUrlsParsed >= 990:
                        self.lock.release()
                        break
                    self.lock.release()
                    self.lock.acquire()
                    if self.totalUrls >= 990 or not robotAllowed:
                        print(
                            "self.totalUrls after parsing ===============", self.totalUrls)
                        self.lock.release()
                        break
                    self.lock.release()
                    try:
                        fullUrl = self.normalizeUrl(baseUrl, a['href'])
                        if fullUrl is not None:
                            if fullUrl not in self.visitedUrlMap:
                                newUrls.append(
                                    (fullUrl, self.checkRobotsSafe(fullUrl)))
                                self.lock.acquire()
                                self.totalUrlsParsed += 1
                                print("self.totalUrlsParsed ===========",
                                      self.totalUrlsParsed)
                                self.lock.release()
                    except:
                        continue
                self.lock.acquire()
                print("total urls to add ========", len(newUrls))
                self.addUrlsToCrawl(newUrls, True, depth + 1, log)
                self.lock.release()
        except Exception as e:
            print("Exception occured =", e)
            self.lock.release()
            return


if __name__ == '__main__':
    searchResults = search("amazon web services", stop=10)
    crawler = Crawler(990)
    log = Log()

    searchResults = [(url, crawler.checkRobotsSafe(url))
                     for url in searchResults]

    crawler.addUrlsToCrawl(searchResults, False, 0, log)

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
    print("totalLogWritesErrors =====", log.totalLogWriteErrors)
