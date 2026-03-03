from scrapers.wootware import scrape_wootware
from scrapers.evetech import scrape_evetech
from scrapers.progenix import scrape_progenix
from scrapers.computermania import scrape_computermania
from scrapers.incredible import scrape_incredible
from scrapers.dreamware import scrape_dreamware
from scrapers.pc_international import scrape_pc_international

# Maps the config string names to actual functions
SCRAPER_MAP = {
    "scrape_wootware": scrape_wootware,
    "scrape_evetech": scrape_evetech,
    "scrape_progenix": scrape_progenix,
    "scrape_computermania": scrape_computermania,
    "scrape_incredible": scrape_incredible,
    "scrape_dreamware": scrape_dreamware,
    "scrape_pc_international": scrape_pc_international,
}
