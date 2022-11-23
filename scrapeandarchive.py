import tabbycatscraper
import archivetab


class ScrapeAndAchiveTab:
    
    def __init__(self, tabs, hostname, database, username, pwd, port_id):
        
        # tabs should be a dictionary with keys path, name, date, comp_type, region
        # date should be type datetime while eveything else shoould be string
        
        self.tabs = tabs
        self.hostname = hostname
        self.database = database
        self.username = username
        self.pwd = pwd
        self.port_id = port_id
        
    def scrape_and_archive(self):
        
        try:
            tab = tabbycatscraper.TabbycatScraper(self.tabs["path"]).get_tab()
        except:
            print(f"could not scrape {self.tabs['name']}")
            return            
        
        with archivetab.TabDatabase(self.hostname, self.database, self.username, self.pwd, self.port_id) as db:
            try:
                db.archive_tab(tab, self.tabs["name"], self.tabs["date"], self.tabs["comp_type"], self.tabs["region"])
            except:
                print(f"could not archive tab of {self.tabs['name']}")
                pass
            
            try:
                db.archive_motions(self.tabs["name"], self.tabs["date"], self.tabs["comp_type"], self.tabs["region"])
            except:
                print(f"could not archive motions of {self.tabs['name']}")
                pass

        