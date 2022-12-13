#!/usr/bin/env python3

from stl.command.stl_command import StlCommand
from multiprocessing import Pool
import pandas, json, os
from pathlib import Path
from datetime import datetime, date, timedelta

def getListings(query):
    print(f'getListings: {query}')
    arguments = {'--all': False,
        '--checkin': None,
        '--checkout': None,
        '--priceMax': None,
        '--priceMin': None,
        '--roomTypes': None,
        '--storage': None,
        '--updated': '1d',
        '--verbose': False,
        '<listingId>': None,
        '<query>': query,
        'calendar': False,
        'data': False,
        'pricing': False,
        'search': True}

    StlCommand(arguments).execute()

def getOccupations(filename):
    filepath = Path(__file__).with_name(filename + '.csv')
    csvfile = pandas.read_csv(filepath)
    listListingIDs=list(csvfile["id"].values)
    Occupation = dict()
    OccupationDates = dict()

    today = date.today()
    
    daysIntoFutureToCheck = 14

    # listListingIDs = ('754108164094419000', '754108164094419000')

    for id in listListingIDs:
        
        OccupationDates['id'] = id
        lengthOfStay = 1
        i = 1 

        try:
            while i < daysIntoFutureToCheck:

                checkin = today + timedelta(days=i)
                checkout = checkin + timedelta(days=lengthOfStay)

                arguments = {
                    '--all': False,
                    '--checkin': checkin.strftime("%Y-%m-%d"),
                    '--checkout': checkout.strftime("%Y-%m-%d"),
                    '--priceMax': None,
                    '--priceMin': None,
                    '--roomTypes': None,
                    '--storage': None,
                    '--updated': '1d',
                    '--verbose': False,
                    '<listingId>': id,
                    '<query>': None,
                    'calendar': False,
                    'data': False,
                    'pricing': True,
                    'search': False
                    }

                try:
                    response = StlCommand(arguments).execute()
                    print(f'{id} - {checkin.strftime("%Y-%m-%d")} - {checkout.strftime("%Y-%m-%d")} - {lengthOfStay} - {response}')
                    datatemp = str(response).replace('None', "'None'")
                    data = json.loads(datatemp.replace("'", "\""))

                except ValueError as response:
                    datatemp = str(response).replace('None', "'None'")
                    d = json.loads(datatemp.replace("'", "\""))
                    data = d['errorTitle']
                    print(f'{id} - {checkin.strftime("%Y-%m-%d")} - {checkout.strftime("%Y-%m-%d")} - {lengthOfStay} - {data}')

                if data == "404":
                    OccupationDates[checkin.strftime("%Y-%m-%d")] = "Missing"
                elif data == "The minimum number of nights has changed": # Minimum nights requirement not met
                    if lengthOfStay < 7:
                        lengthOfStay += 1
                        i -= 1
                    else:
                        for j in range(lengthOfStay):
                            adjcheckin = checkin + timedelta(days=j)
                            OccupationDates[adjcheckin.strftime("%Y-%m-%d")] = "ErrorMinStay"
                elif data == 'None':
                    # Don't know what error this is.
                    if lengthOfStay < 7:
                        lengthOfStay += 1
                        i -= 1
                    else:
                        for j in range(lengthOfStay):
                            adjcheckin = checkin + timedelta(days=j)
                            OccupationDates[adjcheckin.strftime("%Y-%m-%d")] = "ErrorListingUnknown"
                elif data == "This place is no longer available": # Booked out or deactivated
                    for j in range(lengthOfStay):
                        adjcheckin = checkin + timedelta(days=j)
                        # Check if there is already a date that is only a checkout date so it gets not overwritten
                        if OccupationDates.get(adjcheckin.strftime("%Y-%m-%d")) is None:
                            OccupationDates[adjcheckin.strftime("%Y-%m-%d")] = "NA"
                else:
                    for j in range(lengthOfStay):
                        adjcheckin = checkin + timedelta(days=j)
                        OccupationDates[adjcheckin.strftime("%Y-%m-%d")] = data['price_nightly']
                i += 1
        except:
            OccupationDates[today.strftime("%Y-%m-%d")] = "Error"
            print(f'*** Interrupted/Error ***')

        Occupation[id] = OccupationDates
        OccupationDates = dict()

    df = pandas.DataFrame.from_dict(Occupation, orient="index")
    df.to_csv('./data/' + today.strftime("%Y-%m-%d-") + filename + '_occupation.csv', encoding='utf-8', index=False)

    return Occupation

if __name__ == '__main__':
    # 17:07 til 17:18
    with Pool() as poolListings:
        resultListings = poolListings.map_async(getListings, ['Amed, Bali', 'Ubud, Bali', 'Canggu, Bali'])
        resultListings.wait()
        print('Listings updated', flush=True)

    # Todo: Same folder
    with Pool() as poolOccupation:
        resultOccupation = poolOccupation.map_async(getOccupations, [
            'Amed, Bali',
            'Ubud, Bali',
            'Canggu, Bali'
            ])
        resultOccupation.wait()
        print('Occupations updated', flush=True)

    # Occupation = getOccupations("Amed, Bali")
    # print(Occupation)
    # # Write to file
    # df = pandas.DataFrame.from_dict(Occupation, orient="index")
    # df.to_csv('Occupation.csv', encoding='utf-8', index=False)