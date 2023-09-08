import json
import os.path
import requests
import datetime, time
import pandas
import math, statistics

import logging
import traceback

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)

logger.addHandler(ch)

#--- Trade Calculation
# number of buy limits between each trade 
TRADEINTERVAL = 3

#--- Trade Evaluation Parameters
# minimum raw profit per trade (assuming limit is reached)
MIN_PROFIT = 300000
# minimum % profit per trade
MIN_ROI = 1.1
# max money required to buy to limit
MAX_REQ_CAP = 5000000

CHUNKSIZE = 4 # 4 hours per buy limit
DAYS_SAVED = 3 # number of days of data to collect

MAP_FILE = "mapfile.json"
DATA_FILE = "datafile.json"
OUT_FILE = "outfile.json"

BASE_URL = "http://prices.runescape.wiki/api/v1/osrs"
MAP_ENDPOINT = "/mapping"
HOUR_ENDPOINT = "/1h"

USERHEADER = {
    'User-Agent': 'The Marginator',
    'From': 'PracticalPotato on Discord'
}

TAX = 0.01

def api_request(url, endpoint, params=None, headers=USERHEADER):
    logger.info("Requesting: " + endpoint + " " + str(params or ""))
    response = requests.get(url+endpoint, params = params, headers=headers)
    time.sleep(1)
    return json.loads(response.content.decode('utf-8'))

def strip(dt):
    return dt.replace(minute = 0, second = 0, microsecond = 0)

def update_data(hours):
    # if no map file, retrieve it from API
    if not os.path.isfile(MAP_FILE):
        with open(MAP_FILE, "w") as mf:
            mapraw = api_request(BASE_URL, MAP_ENDPOINT)
            outlist = {}
            for dicvalues in mapraw:
                if "limit" in dicvalues.keys():
                    outlist[dicvalues["id"]] = dicvalues
            mf.write(json.dumps(outlist))
        
    # use to look up items
    with open(MAP_FILE, "r") as mf:
        maplist = json.load(mf)
        
    # load previous data, if exists
    try:
        with open(DATA_FILE, "r") as df:
            datalist = json.load(df)
    except:
        datalist = {}
    
    # retrieve remaining data
    thishour = strip(datetime.datetime.now())
    desireddatetimes = []
    for i in range(hours):
        desireddatetimes.append(thishour - datetime.timedelta(hours=hours-i))
    
    templist = []
    for dic in datalist:
        dt = datetime.datetime.fromtimestamp(dic["timestamp"])
        if dt in desireddatetimes:
            templist.append(dic)
            desireddatetimes.remove(dt)
    datalist = templist
    
    for dt in desireddatetimes:
        datapoint = api_request(BASE_URL, HOUR_ENDPOINT, {"timestamp": int(time.mktime(dt.timetuple()))})
        datalist.append(datapoint)
        
    #datalist = sorted(datalist, key=lambda data: data["timestamp"])
    
    with open(DATA_FILE, "w") as df:
        df.write(json.dumps(datalist))
        
    return maplist, datalist

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]
    

def process_chunk(maplist, chunklist, idlist):
    chunksize = len(chunklist)
    ddict = [x["data"] for x in chunklist]
    df = pandas.DataFrame.from_dict(ddict)
    
    out = {}
    dellist = []
    for id in idlist:
        try:
            iteminfo = df.loc[:][id]
            keys = iteminfo[0].keys()
            itemlist = [dic.values() for dic in iteminfo]

            itemdf = pandas.DataFrame(itemlist)
            itemdf.columns = keys
            
            highs = itemdf.loc[:]["avgHighPrice"]
            lows = itemdf.loc[:]["avgHighPrice"]
            limit = maplist[id]["limit"]
            
            highVol = itemdf.loc[:]["highPriceVolume"]
            lowVol = itemdf.loc[:]["lowPriceVolume"]
            thresh = limit/4
            
            #pass on low volume
            if not all(x > thresh for x in highVol) or not all(x > thresh for x in lowVol):
                dellist.append[id]
                continue
            
            medhigh = statistics.median(highs)
            medlow = statistics.median(lows)
            
            out[id] = [medhigh, medlow]
        except Exception as e:
            pass
        
    if len(dellist) > 0:
        logger.info("#" + len(dellist) + " IDs discarded for low volume")
    idlist = [x for x in idlist if x not in dellist]
    
    return out, idlist

if __name__ == "__main__":
    maplist, datalist = update_data(DAYS_SAVED*8)
    logger.info("Data Updated")
    chunklist = chunks(datalist, CHUNKSIZE) # each chunk is 4 hours
    
    idlist = datalist[0]["data"].keys()
    results = []
    for ch in chunklist:
        logger.info("Processing Chunk")
        res, idlist = process_chunk(maplist, ch, idlist)
        results.append(res)
    
    final_output = {}
    for id in results[0].keys():
        highs = []
        lows = []
        try:
            for chunk in results:
                try:
                    x, y = chunk[id]
                    highs.append(x)
                    lows.append(y)
                except KeyError as e:
                    pass
            
            best_high = max(highs)
            median_low = statistics.median(lows)
            
            margin = math.ceil((best_high*0.99)-median_low)
            roi = (median_low+margin)/median_low
            
            if roi < MIN_ROI:
                logger.info(str(id) + " discarded for low roi of " + str(roi))
                continue
            
            limit = maplist[id]["limit"]
            intlimit = limit*TRADEINTERVAL
            rcap = median_low*intlimit
            
            if rcap > MAX_REQ_CAP:
                logger.info(str(id) + " discarded for high reqCap of " + str(rcap))
            fprofit = margin*intlimit
            
            if fprofit < MIN_PROFIT:
                logger.info(str(id) + " discarded for low profit of " + str(fprofit))
                continue
            
            final_output[id] = {
                "high" : best_high,
                "low" : median_low,
                "margin" : margin,
                "%roi" : roi,
                "limit" : limit,
                "interval limit" : intlimit,
                "req cap" : rcap,
                "profit" : fprofit
            }
        except ValueError as e:
            pass
        except TypeError as e:
            pass
    logger.info("Writing Outfile")
    with open(OUT_FILE, "w") as of:
        of.write(json.dumps(final_output, indent=4))
    pass