from scrapeandarchive import ScrapeAndAchiveTab
import datetime

# list of tabs to scrape. Please use datetime type for date
"""
Lisyt the tabs to archive in the below format.

tabs_to_archive = [{"path": r"https://dnn.calicotab.com/hrd2021/", "name": "Human Rights Debating Championship 2021", "date": datetime.datetime(1, 1, 1), "comp_type": "unknown", "region": "unknown"},
{"path": r"https://ecdo2021.herokuapp.com/ecdo-2021/", "name":	"East Coast Debate Open 2021", "date": datetime.datetime(1, 1, 1), "comp_type": "unknown", "region": "unknown"},
{"path": r"https://mace.calicotab.com/tco2021/", "name":	"The Call Out Debate Open Championships 2021", "date": datetime.datetime(1, 1, 1), "comp_type": "unknown", "region": "unknown"},
{"path": r"https://warm-lake-32380.herokuapp.com/guindon2021/", "name":	"2021 Father Roger Guindon Cup", "date": datetime.datetime(1, 1, 1), "comp_type": "unknown", "region": "unknown"}]

"""

tabs_to_archive = [{}]

for i in tabs_to_archive:
    ScrapeAndAchiveTab(i, "localhost", "postgres", "postgres", "King1996", 5432).scrape_and_archive()
    print(i["name"])
