import urllib, urllib.request
import xmltodict

url = 'http://export.arxiv.org/api/query?search_query=all:climate+change+technology&start=0&max_results=1'
data = urllib.request.urlopen(url)

xml_resp = data.read().decode('utf-8')
resp = xmltodict.parse(xml_resp)
print(resp)