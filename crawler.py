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

    def createLog(self):
        logFile = open(self.fileName, 'w', newline='')
        filewriter = csv.writer(logFile, delimiter=',')
        filewriter.writerow(self.fields)
        return filewriter, logFile

    def writeLog(self, row):
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
        self.lock = threading.Lock()
        self.totalUrls = 0

    def addUrlsToCrawl(self, urls):
        self.lock.acquire()
        for url in urls:
            if url not in self.visitedUrlMap:
                self.visitedUrlMap[url] = 1
                self.totalUrls += 1
                if self.totalUrls <= self.maxLinks:
                    self.priorityQueue.put((-1, url))

        print("priority queue size after insert =====",
              self.priorityQueue.qsize())
        self.lock.release()

    def normalizeUrl(self, base_url, link):
        print("base_url =", base_url)
        print("link =", link)
        base_url = urljoin(base_url, '/')
        if link is None or link == '#' or link.startswith('#'):
            return None
        if '#' in link:
            link = link.split('#', 1)[0]
        if link.startswith('//'):
            return 'http' + link
        if link.startswith('/'):
            return base_url[:-1] + link if base_url[-1] == '/' else base_url + link
        else:
            return base_url + link
        return link

    def crawlWeb(self, log):
        try:
            while not self.priorityQueue.empty():
                print("self.totalUrls =", self.totalUrls)
                print("self.maxLinks =", self.maxLinks)
                self.lock.acquire()
                score, url = self.priorityQueue.get_nowait()
                print(threading.currentThread().getName())
                print(url)
                row = [url, '10', '1', score, datetime.now().strftime(
                    "%d-%m-%Y %H:%M:%S"), threading.currentThread().getName()]

                parsedUrl = urlparse(url)
                domain = parsedUrl.netloc
                baseUrl = parsedUrl.scheme + '://' + domain

                print("domain =", domain)

                if domain in self.visitedDomainsNoveltyScore:
                    self.visitedDomainsNoveltyScore[domain] -= 1
                else:
                    self.visitedDomainsNoveltyScore[domain] = 100
                self.lock.release()
                log.writeLog(row)

                if self.totalUrls > self.maxLinks:
                    continue
                try:
                    page = urlopen(url)
                    mimeType = page.info().get_content_maintype()
                    if mimeType != 'text' or page.getcode() != 200:
                        continue
                    htmlContent = page.read().decode('utf8')
                except Exception as e:
                    print("Exception occured during opening a page =", e)
                    continue

                soup = BeautifulSoup(htmlContent, 'html.parser')
                newUrls = []
                for a in soup("a"):
                    if self.totalUrls > self.maxLinks:
                        break
                    try:
                        print("a['href'] =", a['href'])
                        fullUrl = self.normalizeUrl(baseUrl, a['href'])
                        print("fullUrl =", fullUrl)
                        if fullUrl not in self.visitedUrlMap and fullUrl is not None:
                            newUrls.append(fullUrl)
                    except:
                        continue
                self.addUrlsToCrawl(newUrls)
        except Exception as e:
            print("Exception occured =", e)
            self.lock.release()
            return


if __name__ == '__main__':
    searchResults = search("amazon web services", stop=10)
    crawler = Crawler(1000)
    log = Log()
    crawler.addUrlsToCrawl(searchResults)

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
