import concurrent.futures
import json
import logging
import math
import sys
from collections import defaultdict
from concurrent.futures import ALL_COMPLETED
from datetime import datetime
from time import time
import pandas as pd
import requests
from geopy.geocoders import Photon
from pymongo import MongoClient


def genRows(df):
    for row in df.itertuples(index=False):
        yield row._asdict()


def stripAndSplit(stringValue):
    try:
        return [str(eachString).strip() for eachString in str(stringValue).split(",")]
    except:
        root_logger.warning("Error in stripAndSplit function")
        return None


def parseTime(timeList):
    try:
        updatedtimeList = []
        for eachObj in timeList:
            isOvernight = eachObj['open'] >= eachObj['close']
            updatedtimeList.append(
                {'day': eachObj['day'], 'start': eachObj['open'], 'end': eachObj['close'], 'isOvernight': isOvernight})
        return updatedtimeList
    except:
        root_logger.warning("Error in parseTime function")
        return None


def parseVybe(vybeDataList):
    try:
        if len(vybeDataList) == 0:
            return None
        vybe = {}
        for eachObj in vybeDataList:
            vybe[eachObj["name"]] = eachObj["data"]
        return vybe
    except:
        root_logger.warning("Error in parseVybe function")
        return None


def parseAddress(brokenAddressList):
    try:
        if brokenAddressList is None or not (len(brokenAddressList)):
            return None
        fullAddress = {'landMark': brokenAddressList[0],
                       'streetAddress': brokenAddressList[1],
                       'city': brokenAddressList[3],
                       'zipcode': brokenAddressList[4],
                       'state': brokenAddressList[5],
                       'country': brokenAddressList[6]
                       }
        return fullAddress
    except:
        root_logger.warning("Error in parseAddress function")


def updateRow(row):
    global rId
    vybeRow = {}
    global countriesDict, iterationStatusCode, errorRecordDict
    row["category"] = None
    row['cuisine'] = stripAndSplit(row['cuisine'])
    row['phone'] = stripAndSplit(row['phone'])
    row['__v'] = 0
    row['rating'] = None
    row['tags'] = None
    row['videoLink'] = None
    row['reviewCount'] = None
    row['isClaimed'] = None
    row['faq'] = None
    # row['category'] = None
    row['securityDocumentUrl'] = None
    row['securityDocumentName'] = None
    row['socialMediaHandles'] = {"facebook": None, "instagram": None,
                                 "twitter": None, "snapchat": None, "pinterest": None}
    row['hoursType'] = "REGULAR"
    row['isOpenNow'] = True
    row['specialHours'] = [
        {"date": None, "start": None, "end": None, "isOvernight": None}]
    row['attributes'] = {"businessParking": {"garage": None, "street": None}, "genderNeutralRestrooms": None,
                         "openToAll": None, "restaurantsTakeOut": None, "wheelChairAccessible": None}

    postRestaurantName = row['name']
    postAddress = row['oldAddress']
    postCity = row["area"].split(",")[-1]
    url = "https://qozddvvl55.execute-api.us-east-1.amazonaws.com/Prod/vybe/"
    payload = {
        "name": postRestaurantName,
        "address": postAddress
    }
    headers = {'Content-Type': 'text/plain'}

    for i in range(5):
        if i == 1:
            payload['address'] = postCity
        elif i == 2:
            payload['address'] = postAddress.split(",")[-1]
        elif i == 3:
            params = {'name': postRestaurantName, "city": postCity,
                      'address1': postAddress, 'state': stateName, 'country': "US"}

            yelpReq = requests.get(yelpUrl, params=params, headers=head)
            try:
                address = " ".join(
                    yelpReq.json()["businesses"][0]["location"]["display_address"])
                payload['address'] = address
            except:
                root_logger.warning(
                    f"Yelp can't find the address for {payload['name']}")
                continue
        elif i == 4:
            try:
                geolocator = Photon(user_agent="My-app")
                location = geolocator.geocode(postAddress)
                if location is not None:
                    payload['address'] = location.address
                else:
                    root_logger.warning(
                        f"Photon could not find address for {payload['name']}")
            except Exception as e:
                root_logger.warning(f"Photon failed for {payload['name']}")
                root_logger.exception(e)
        response = requests.request(
            "POST", url, headers=headers, data=json.dumps(payload))
        statusCode = response.status_code
        root_logger.info(
            f"{statusCode} - ({postRestaurantName}) - ({payload['address']}) iteration ({i})")
        if statusCode == 200:
            iterationStatusCode[str(i)] += 1
        if statusCode == 200 or statusCode == 502:
            break

    if statusCode == 200:
        res = response.json().get('message')
        restaurantId = 'RestId' + datetime.now().strftime("%d%m%y%H%M") + \
            str(rId).zfill(3)
        # increase this if want to use threading to execute more than 1000 records at a time
        rId = (rId + 1) % 1000
        hour = parseTime(res.get('timings'))
        website = res.get('website')
        address = parseAddress(res.get('address_broken'))
        coordinates = {"coordinates": [res.get('coordinates').get(
            "longitude"), res.get('coordinates').get("latitude")], "type": "Point"}
        newAddress = res.get('address')
        vybe = parseVybe(res.get('vybe'))

        if vybe:
            minTimeSpent = res.get('timeSpent')[
                0] if res.get('timeSpent') else None
            maxTimeSpent = res.get('timeSpent')[
                1] if res.get('timeSpent') else None
            vybeRow.update({"restaurantId": restaurantId, 'vybe': vybe,
                            'minTimeSpent': minTimeSpent, 'maxTimeSpent': maxTimeSpent})
            countriesVybeDict.append(vybeRow)
        else:
            root_logger.warning("{} No VybeData ( {} ) ( {} )".format(
                statusCode, postRestaurantName, postAddress))

        row.update({"restaurantId": restaurantId, "location": coordinates,
                    "newAddress": newAddress, "hours": hour, "website": website, "address": address})

        countriesDict.append(row)
        #print(countriesDict)

    else:
        hour = None
        website = None
        address = None
        newAddress = None
        address = None
        row.update({"newAddress": newAddress, "hours": hour,
                    "website": website, "address": address})
        errorRecordDict.append(row)
        root_logger.info("{} Entered restaurant with name ( {} ) and address( {} ) into error file".format(
            statusCode, postRestaurantName, postAddress))
    return statusCode


if __name__ == "__main__":

    '''with open("./credentials.txt") as file:
        api_key = file.readline().strip()

    head = {'Authorization': 'Bearer %s' % api_key}
    yelpUrl = 'https://api.yelp.com/v3/businesses/matches'''
    api_key='zqVXmSzDDGY7pCmoLAP6h83vAYPHmU8Id9gXqFOeaB9NaxW8tdOpD1mjoSlwGfCfe31A9WfsV1My9ekOajXqc1KnzCO4M6Zn3JXkss1-Yzw_Pmw6h54hmsxO6CkUX3Yx'
    head = {'Authorization': 'Bearer %s' % api_key}
    yelpUrl='https://api.yelp.com/v3/businesses/matches'

    countriesDict = []
    countriesVybeDict = []
    errorRecordDict = []
    rId = 0
    iterationStatusCode = {'0': 0, '1': 0, '2': 0, '3': 0, '4': 0}

    presentTime = datetime.now().strftime("%d_%m_%Y_%H_%M")
    logName = "TNRestaurant" + presentTime + ".log"
    # logging.basicConfig(filename=logName,filemode='w', encoding='utf-8', level=logging.INFO, format="( %(asctime)s ) - %(levelname)s - %(message)s",datefmt='%Y-%m-%d %H:%M:%S')
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    handler = logging.FileHandler(logName, 'w', 'utf-8')
    handler.setFormatter(logging.Formatter(
        "( %(asctime)s ) - %(levelname).4s - %(message)s", "%Y-%m-%d %H:%M:%S"))
    root_logger.addHandler(handler)

    with open('srv.txt', 'r') as f:
        fileName = f.readline().strip()
        client = MongoClient(f.readline().strip())
        db = client[f.readline().strip()]
        errorExcelSheetName = f.readline().strip()
        stateName = f.readline().strip()

    df = pd.read_excel(fileName, engine='openpyxl')
    # df = pd.read_excel(fileName, engine='openpyxl',
    #                    skiprows=range(100, len(df_all.index)))

    required = {'Category': 'cuisine', 'Address': 'oldAddress', 'Name': 'name', 'Area': 'area', 'Price': 'priceRange', 'Phone': 'phone'}

    required_keys=required.keys()
    original_column=df.columns
    for i in original_column:
        if i in required_keys:
            df.rename(columns={i:required[i]},inplace=True)
        else:
            df.drop(i,axis=1,inplace=True)
            root_logger.warning(f"{i} column is dropped")

    # if len(required) > 0:
    #     root_logger.warning(f"{required} columns are missing")
    #     exit()


    df['name'] = df['name'].str.strip()
    row = genRows(df)
    apiStatusCode = defaultdict(int)
    
    numOfThreads = 200
    threadBatch = 1
    totalExcelRows = len(df.index)
    with concurrent.futures.ThreadPoolExecutor() as executor:
        for i in range(math.ceil(totalExcelRows / numOfThreads)):
            root_logger.info(f"{threadBatch} thread Batch runnning \n")
            if totalExcelRows > numOfThreads:
                k = numOfThreads
                totalExcelRows = totalExcelRows - numOfThreads
            else:
                k = totalExcelRows
            try:
                startTimeForOneBatch = time()
                future = [executor.submit(updateRow, next(row))
                          for i in range(k)]
                print(f"Threads Running - {len(future)}")
                for thread in future:
                    try:
                        statusCode = thread.result()
                        apiStatusCode[statusCode] += 1
                    except StopIteration as e:
                        break
                    except Exception as e:
                        root_logger.exception(e)
                        continue
                for statusCodeKey, countValue in apiStatusCode.items():
                    root_logger.warning(
                        f"{countValue} rows generated: STATUS CODE - {statusCodeKey}")
                    print(
                        f"{countValue} rows generated:- STATUS CODE - {statusCodeKey}")
                threadBatch += 1
            except StopIteration as e:
                break
            except Exception as e:
                root_logger.exception(e)
                if (concurrent.futures.wait(future, timeout=None, return_when=ALL_COMPLETED)):
                    break
            timeTakenToFinishOneBatch = time()
            print("time = {} taken for thread batch = {}".format(
                round(timeTakenToFinishOneBatch - startTimeForOneBatch, 2),
                threadBatch - 1))
    print(apiStatusCode)

    try:
        collectionName = "Georgia_Restaurant" + presentTime
        with open(collectionName + '.json', 'a', encoding='utf-8') as f:
            json.dump(countriesDict, f)
        collectionToBeInserted = db[collectionName]
        collectionToBeInserted.insert_many(countriesDict)
        print("Created Collection ", collectionName)
        root_logger.info("Created Collection {}".format(collectionName))
    except Exception as e:
        root_logger.warning(e)

    try:
        collectionName = "Georgia_Vybe"+presentTime
        with open(collectionName + '.json', 'a', encoding='utf-8') as f:
            json.dump(countriesVybeDict, f)
        collectionToBeInserted = db[collectionName]
        collectionToBeInserted.insert_many(countriesVybeDict)
        print("Created Collection ", collectionName)
        root_logger.info("Created Collection {}".format(collectionName))
    except Exception as e:
        root_logger.warning(e)

    errorExcelDataFrame = pd.DataFrame.from_dict(errorRecordDict)
    errorExcelDataFrame.to_excel(errorExcelSheetName + presentTime + ".xlsx")
