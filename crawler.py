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
        # stores the number of writes into the csv file
        self.totalLogWrites = 0

        # used to maintain the total number of errors while writing the logs
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
    def __init__(self, maxUrls):
        # maximum URLs that should be parsed
        self.maxUrls = maxUrls

        # stores a url with novelty and importance score
        self.priorityQueue = PriorityQueue()

        # stores visited urls
        self.visitedUrlMap = {}

        # stores the information if the url is allowed to parse based on robots.txt file
        self.robotAllowed = {}

        # stores the novelty score for the domain
        self.visitedDomainsNoveltyScore = {}

        # stores the importance score for the domain
        self.importanceScore = {}

        # stores the number of URLs parsed in different pages
        self.totalUrlsParsed = 0

        self.lock = threading.Lock()

        # stores all the URLs that are added to the priority queue
        self.totalUrls = 0

        # stores the count of the URLs that are visited again
        self.totalDuplicatedUrls = 0

    # adds URLs to priority queue
    # newUrlsFlag will be True if the url is not visited before else False
    # newUrlsFlag is False when the same url is added to the priority queue after
    # updating the novelty score
    def addUrlsToCrawl(self, urls, newUrlsFlag, depth, log):
        try:
            for (url, robotAllowed) in urls:
                if url not in self.visitedUrlMap:
                    self.visitedUrlMap[url] = 1
                    if newUrlsFlag:
                        if self.totalUrls >= self.maxUrls:
                            return
                        self.totalUrls += 1
                        print("total new URLs =", self.totalUrls)
                        parsedUrl = urlparse(url)
                        domain = parsedUrl.netloc
                        if domain not in self.visitedDomainsNoveltyScore:
                            self.visitedDomainsNoveltyScore[domain] = (
                                100, datetime.now())
                        else:
                            # decrease novelty score if the url of the same domain is visited again
                            noveltyScore = self.visitedDomainsNoveltyScore[domain][0]
                            self.visitedDomainsNoveltyScore[domain] = (
                                noveltyScore - 5, datetime.now())
                        if url not in self.importanceScore:
                            self.importanceScore[url] = 10
                        else:
                            self.importanceScore[url] += 10

                    if newUrlsFlag is False or self.totalUrls <= self.maxUrls:
                        self.priorityQueue.put(
                            (self.calculateScore(url) * -1, (url, datetime.now(), depth, robotAllowed)))
                else:
                    # decrease the totalUrlsParsed variable as we need more URLs to get total
                    # maxLinks
                    self.totalUrlsParsed -= 1
                    self.totalDuplicatedUrls += 1
                    print("URL already visited =", url)
        except Exception as e:
            print("exception occured while adding URLs = ", e)
        print("priority queue size = ", self.priorityQueue.qsize())

    # normalize URL such that every URL has a domain present in the URL
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

    # returns the score for the URL based upon importance and novelty factors
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

    # checks if the URL is safe to parse the page based on robots.txt
    def checkRobotsSafe(self, url):
        print("robot safe api called")
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
        print("checkRobotSafe Total Time =",
              datetime.now() - start)
        return isAllowed

    def crawlWeb(self, log):
        try:
            while not self.priorityQueue.empty():
                self.lock.acquire()
                maximumUrls = self.maxUrls
                if log.totalLogWrites >= self.maxUrls + 10:
                    self.lock.release()
                    break

                url = ""
                count = 0
                # find the most relevant URL based on importance and novelty score
                while True:
                    score, (newUrl, timeStamp,
                            depth, robotAllowed) = self.priorityQueue.get_nowait()

                    count += 1
                    parsedUrl = urlparse(newUrl)
                    domain = parsedUrl.netloc
                    baseUrl = parsedUrl.scheme + '://' + domain

                    # visitedDomainsNoveltyScore stores updated timestamp for each domain.
                    # if the timestamp of current URL is greater than the domain timestamp then
                    # it means that the novelty score is not updated after the URL is added into the
                    # priority queue and it is safe to assume that the URL is highly relevant.
                    # The other case is if the priority queue url timestamp is less than domain timestamp for
                    # novelty score then the url is added back to the priority queue with the updated novelty
                    # score. If next time same url occurs in the priority queue then it is safe to assume that
                    # the url is highly relevant.
                    if newUrl == url or timeStamp >= self.visitedDomainsNoveltyScore[domain][1]:
                        break

                    # add url back to priority queue with updated score
                    del self.visitedUrlMap[newUrl]
                    self.addUrlsToCrawl(
                        [(newUrl, robotAllowed)], False, depth, log)
                    url = newUrl
                print(
                    "no of times the priority queue updated for highly relevant URL =", count)
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
                    log.writeLog(row, self.maxUrls + 10)

                    if mimeType != 'text' or statusCode != 200:
                        continue

                except HTTPError as e:
                    print("Exception occured during opening a page ",
                          newUrl, " = ", e)
                    statusCode = e.code
                    row = [newUrl, 0, depth, score*-1, statusCode, datetime.now().strftime(
                        "%d-%m-%Y %H:%M:%S"), robotAllowed, threading.currentThread().getName()]
                    log.writeLog(row, self.maxUrls + 10)
                    continue
                except URLError as e:
                    print("URL ERROR OCCURRED =", e)
                    row = [newUrl, 0, depth, score*-1, -1, datetime.now().strftime(
                        "%d-%m-%Y %H:%M:%S"), robotAllowed, threading.currentThread().getName()]
                    log.writeLog(row, self.maxUrls + 10)
                    continue
                except Exception as e:
                    print("Exception OCCURRED = ", e)
                    row = [newUrl, 0, depth, score*-1, -1, datetime.now().strftime(
                        "%d-%m-%Y %H:%M:%S"), robotAllowed, threading.currentThread().getName()]
                    log.writeLog(row, self.maxUrls + 10)
                    continue

                self.lock.acquire()
                # don't add more URLs if the priority queue has maxUrls
                if self.totalUrls >= maximumUrls or not robotAllowed:
                    print("totalUrls added in priority queue =", self.totalUrls,
                          " robot allowed to fetch url ", newUrl, " = ", robotAllowed)
                    self.lock.release()
                    continue
                self.lock.release()

                # parse the page to find more URLs
                soup = BeautifulSoup(htmlContent, 'html.parser')
                newUrls = []
                anchorTags = soup("a")
                print("total anchorTags in url ",
                      newUrl, " = ", len(anchorTags))
                for a in anchorTags:
                    self.lock.acquire()
                    # don't add more URLs in priority queue if the number of URLs added or about to add in the
                    # priority queue by different threads reaches maxUrls.
                    if self.totalUrlsParsed >= maximumUrls:
                        self.lock.release()
                        break
                    self.lock.release()
                    self.lock.acquire()
                    if self.totalUrls >= maximumUrls or not robotAllowed:
                        print(
                            "total URLs added to the priority queue = ", self.totalUrls)
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
                                print("total URLs parsed =",
                                      self.totalUrlsParsed)
                                self.lock.release()
                    except:
                        continue
                self.lock.acquire()
                print("total urls to add =", len(newUrls))
                self.addUrlsToCrawl(newUrls, True, depth + 1, log)
                self.lock.release()
        except Exception as e:
            print("Exception occured =", e)
            self.lock.release()
            return


def checkRobotSafeForInitialSearchResults(crawler, url, robotSafeSearchResults):
    isAllowed = crawler.checkRobotsSafe(url)
    robotSafeSearchResults.append((url, isAllowed))


if __name__ == '__main__':
    query = input("Enter Search query: ")
    maxUrls = input("Enter max links: ")
    searchResults = search(query, stop=10)
    crawler = Crawler(int(maxUrls) - 10)
    log = Log()
    robotSafeSearchResults = []
    threads = []

    for i, url in enumerate(searchResults):
        thread = threading.Thread(
            target=checkRobotSafeForInitialSearchResults, name="name"+str(i), args=(crawler, url, robotSafeSearchResults))
        threads.append(thread)
        thread.start()

    # wait for all threads to finish
    for thread in threads:
        thread.join()

    crawler.addUrlsToCrawl(robotSafeSearchResults, False, 0, log)

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
    print("total duplicated urls which are already visited = ",
          crawler.totalDuplicatedUrls)
    print("totalLogWrites calls =", log.totalLogWrites)
    print("totalLogWritesErrors =", log.totalLogWriteErrors)
