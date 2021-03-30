
import requests
import statistics as stat
import csv
import os.path


def welcome(name):
    print("Welcome to {0}".format(name))
    return 0


def get_weather_stormglass(event):
    if len(event['start']) != 10:
        print('Start date is wrong!')
        return

    if len(event['end']) != 10:
        print('End date is wrong!')
        return

    #event["start"] = arrow.get(event["start"])
    #event["end"] = arrow.get(event["end"])

    event["url"] = findapipair('APIendpoints.csv', 'stormglass')
    if event["url"] == 0:
        print('URL not found!')
        return

    event["key"] = findapipair('APIkeys.csv', 'stormglass')
    if event["key"] == 0:
        print('API key not found!')
        return

    response = requests.get(
        event["url"],
        params={
            'lat': event["lat"],
            'lng': event["lng"],
            'start': event["start"],
            'end': event["end"],
            'params': ','.join(event["env_params"]),
        },
        headers={
            'Authorization': event["key"]
        }
    )

    return response


# def get_weather_rapidapi(lat, long, start, end):
#     url = "https://community-open-weather-map.p.rapidapi.com/weather"
#     querystring = {"q": "London,uk", "lat": lat, "lon": long, "callback": "test", "id": "2172797", "lang": "null",
#                    "units": "\"metric\" or \"imperial\"", "mode": "xml, html"}
#     headers = {
#         'x-rapidapi-key': "f6eecb6559mshb3a95af04b3a397p1b8c6djsn88d5a8335307",
#         'x-rapidapi-host': "community-open-weather-map.p.rapidapi.com"
#     }
#     response = requests.request("GET", url, headers=headers, params=querystring)
#     return response


def db_store(fileprefix, weather, header_keys):
    filename = fileprefix + weather['time'] + '.csv'
    exists = os.path.isfile(filename)
    if not exists:
        with open(filename, 'a+') as file:
            writer = csv.DictWriter(file, fieldnames=header_keys)
            writer.writeheader()

    with open(filename, 'a+') as file:
        writer = csv.DictWriter(file, fieldnames=header_keys)
        writer.writerow(weather)
    return


def db_load(filename):
    with open(filename, 'r') as file:
        file.seek(0)
        weather_db = file.read()
    return weather_db


def findapipair(file, key):
    with open(file,'r') as api_ep:
        filelines = api_ep.readlines()
        for line in filelines:
            if key in line.lower():
                value = line.split(",")[1].replace("\n", "")
                return value.replace(" ","")
            else:
                return 0


def getparams(pfile):
    with open(pfile, 'r') as env_params:
        filelines = env_params.readlines()
        for i, line in enumerate(filelines):
            filelines[i] = filelines[i].replace(",\n", "")
    return filelines


def chew_data(response, API, env_params):

    # Average over all sources
    response = response['hours'][0]
    newDict = {}
    for key in response:
        if key != 'time':
            av = []
            for inner_key in response[key]:
                av.append(response[key][inner_key])
            newDict[key] = stat.mean(av)

    newDict.update({'time': response['time']})
    return newDict


def load_route(routefile):
    newList = []
    with open(routefile, 'r') as rfile:
        filelines = rfile.readlines()
        keys = filelines[0].split(";")
        keys = [x.replace("\n", "") for x in keys]
        for i, line in enumerate(filelines):
            if i > 0:
                newDict = {}
                filelines[i] = filelines[i].split(";")
                filelines[i] = [x.replace("\n", "") for x in filelines[i]]
                for m, key in enumerate(keys):
                    if key == "Latitude":
                        convertedLat = filelines[i][m]
                        convertedLat = convertedLat.replace('N', '')
                        convertedLat = convertedLat.replace('S', '-')
                        newDict.update({key: float(convertedLat)})
                    elif key == "Longitude":
                        convertedLng = filelines[i][m]
                        convertedLng = convertedLng.replace('E', '')
                        convertedLng = convertedLng.replace('W', '-')
                        newDict.update({key: float(convertedLng)})
                    else:
                        newDict.update({key: filelines[i][m]})
                newList.append(newDict)

    return newList

if __name__ == '__main__':

    ################ USER ####################
    API = "stormglass"
    pfile = 'env_params.csv'
    rfile = 'SaoPaolo_Rotterdam.csv'
    start_date = '2021-03-15'
    end_date = '2021-03-15'
    data_range = [15, 17]
    ################ USER END ################

    inquiries = load_route(rfile)
    inquiries = inquiries[data_range[0]:data_range[1]]
    env_params = getparams(pfile)
    des_items = []
    des_items.extend(list(inquiries[0].keys()))
    des_items.extend(env_params)
    des_items.append('time')

    if input("Sending " + str(data_range[1]-data_range[0]) + " requests to " + API + ". Proceed? (y) ").lower() == 'y':
        for i, event in enumerate(inquiries):
            print("Request " + str(i+1) + " of " + str(data_range[1]) + ": ", end='')
            req_param = {'lat': event['Latitude'], 'lng': event['Longitude'], 'start': start_date, 'end': end_date, 'env_params': env_params}
            res_stormglass = get_weather_stormglass(req_param)
            if not res_stormglass == []:
                print('API Status ' + str(res_stormglass.status_code), end='')
                readable = chew_data(eval(res_stormglass.text), API, req_param["env_params"])
                event.update(readable)
                db_store('SP_RO/', event, des_items)
                print(' - Success.')
            else:
                print('Stopped.')


#event = {"lat": -24.7858, "lng": -43.8593, "start": "2021-03-23", "end": "2021-03-23", "env_params": getparams(pfile)}

# # Do something with response data.
#     json_data = response.json()
#
#     url = "https://community-open-weather-map.p.rapidapi.com/weather"
#     querystring = {"q": "London,uk", "lat": lat, "lon": long, "callback": "test", "id": "2172797", "lang": "null",
#                    "units": "\"metric\" or \"imperial\"", "mode": "xml, html"}
#     headers = {
#         'x-rapidapi-key': "f6eecb6559mshb3a95af04b3a397p1b8c6djsn88d5a8335307",
#         'x-rapidapi-host': "community-open-weather-map.p.rapidapi.com"
#     }
#     response = requests.request("GET", url, headers=headers, params=querystring)
#     return response
