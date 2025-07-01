from owslib.wcs2 import WebCoverageService

url = "https://service.pdok.nl/rws/hoogte/dtm/wcs/v1_0"
wcs = WebCoverageService(url, version="2.0.1")

print(list(wcs.contents))