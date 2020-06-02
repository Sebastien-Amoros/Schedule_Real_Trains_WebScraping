#!/usr/bin/env python
# coding: utf-8

import requests
import time
import random
import json
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, date
from fake_useragent import UserAgent
import itertools as it

#Listes des gares
gares_OCE = {'nancy' : '87141002' , 'strasbourg' : '87212027' , 'molsheim' : '87214577' , 'saint-die-des-vosges' : '87144014' , 'arras' : '87342014' , 
            'bethune' : '87342006' , 'etaples-le-touquet' : '87317065', 'amiens' : '87313874' , 'abbeville' : '87317362' , 'compiegne' : '87276691' , 
            'creil' : '87276006' , 'tergnier' : '87296442' , 'lens' : '87345025' , 'hirson' : '87295063'}

gares_dim = {'nancy' : 'NCY' , 'strasbourg' : 'STG' , 'molsheim' : 'MOL' , 'saint-die-des-vosges' : 'STD' , 'arras' : 'ARR' , 
            'bethune' : 'BET' , 'etaples-le-touquet' : 'ETA', 'amiens' : 'AMS' , 'abbeville' : 'ABB' , 'compiegne' : 'CPE' , 
            'creil' : 'CLH' , 'tergnier' : 'TGR' , 'lens' : 'LNS' , 'hirson' : 'HIQ'}

# Authentication token
api_auth = {1 : 'YOUR TOKEN' , 2 : 'YOUR TOKEN' , 3 : 'YOUR TOKEN'}

nb_api = 1

s = requests.session()


def init_VPN():  
    ##VPN##
    #On lit la liste des VPN
    proxies = pd.read_csv('VPN_list.txt', header = None)
    #On créé une liste de VPN
    proxies = proxies.values.tolist()
    #On fait en sorte que cette liste puisse être itérative
    proxies = list(it.chain.from_iterable(proxies))
    #On mélange les VPN
    random.shuffle(proxies)
    #On créé un cycle de VPN
    proxy_pool = it.cycle(proxies)
    #On sélectionne un VPN
    proxy = next(proxy_pool)
    return(proxy)


def main():
    
    #Prévoir requête par train

    count = 0
    nb_api = 1

    #On teste si le VPN fonctionne
    proxy = init_VPN()
    #print(proxy)
    try:
        ua = UserAgent()
        s = requests.session()
        page_test = s.get("https://google.com/", 
                                 proxies={"https": 'socks5://YOUR-IDENT:YOUR-PASSWORD'+proxy}, 
                                 headers={'User-Agent': ua.random},
                                 timeout=time.sleep(random.randrange(1,5)))
        #print(page_test.status_code)
    except requests.exceptions.RequestException as e:
        #print(proxy+" - NOK")
        proxy = init_VPN()
        #print(proxy)

    #On boucle pour réaliser une requête à chaque gare de la liste gares_OCE
    for OCE in gares_OCE:
        #On exécute le prog sur une station donnée
        data_scrap_to_table(OCE, proxy)
        #On compte pour équilibrer le nbr de requêtes sur les 3 clés API
        count = count + 1
        if count == 5:
            nb_api = 2
        elif count == 10:
            nb_api = 3



def data_SNCF(request_url, items, proxy):

    #init variable
    headsign=[]
    direction=[]
    commercial_mode=[]
    mission=[]
    arrival_date_time=[] 
    base_arrival_date_time=[]
    departure_date_time=[]
    base_departure_date_time=[]
    
    # API SNCF
    ua = UserAgent()
    request_result = s.get(request_url, auth=(api_auth[nb_api] ,  ''), 
                           proxies={"https": 'socks5://YOUR-IDENT:YOUR-PASSWORD'+proxy}, 
                           headers={'User-Agent': ua.random},
                           timeout=time.sleep(random.randrange(1,5)))
  
    # context
    context = request_result.json()["context"]
    # context sub-node: current date time
    cur_date = context["current_datetime"]
    
    # pagination
    pagination = request_result.json()["pagination"]
    # context sub-node: pagination
    items_on_page = pagination["items_on_page"]
    
    # departures
    dep_or_arr = request_result.json()[items]
    
    for j in range(0,items_on_page-1):
    
        # Display informations
        direction.append(str(dep_or_arr[j]["display_informations"]["direction"]))
        commercial_mode.append(str(dep_or_arr[j]["display_informations"]["commercial_mode"]))
        mission.append(str(dep_or_arr[j]["display_informations"]["label"]))

        # headsign
        headsign.append(int(dep_or_arr[j]["display_informations"]["headsign"]))

        # stop_date_time
        try:
            arrival_date_time.append(datetime.strptime(dep_or_arr[j]["stop_date_time"]["arrival_date_time"],"%Y%m%dT%H%M%S"))
        except:
            arrival_date_time.append('00000000T000000')
        try:
            base_arrival_date_time.append(datetime.strptime(dep_or_arr[j]["stop_date_time"]["base_arrival_date_time"],"%Y%m%dT%H%M%S"))
        except:
            base_arrival_date_time.append('00000000T000000')
        try:
            base_departure_date_time.append(datetime.strptime(dep_or_arr[j]["stop_date_time"]["base_departure_date_time"],"%Y%m%dT%H%M%S"))
        except:
            base_departure_date_time.append('00000000T000000')
        try:
            departure_date_time.append(datetime.strptime(dep_or_arr[j]["stop_date_time"]["departure_date_time"],"%Y%m%dT%H%M%S"))
        except:
            departure_date_time.append('00000000T000000')
        
    dict_dep = {"headsign" : headsign, "direction" : direction, "commercial_mode" : commercial_mode, "mission" : mission, "arrival_date_time" : arrival_date_time, "base_arrival_date_time" : base_arrival_date_time, "departure_date_time" : departure_date_time, "base_departure_date_time": base_departure_date_time}
    return dict_dep



def data_scrap_to_table(gare, proxy):

    import sqlite3

    #On réalise la connexion vers SQLITE. Une base pour les départs et une base pour les arrivées
    conn = sqlite3.connect("data.db")

    cur = conn.cursor()
    
    stop_area = gare

    #Request URL
    request_url = "https://api.sncf.com/v1/coverage/sncf/stop_areas/stop_area:OCE:SA:"+gares_OCE[stop_area]

    #On test si les tables existent sinon création des tables.
    cur.execute("CREATE TABLE IF NOT EXISTS data (id INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE, stop_area TEXT, type TEXT, headsign INTEGER, direction TEXT, commercial_mode TEXT, mission TEXT, platform TEXT, infos TEXT, retard TEXT, arrival_date_time DATE, base_arrival_date_time DATE, departure_date_time DATE, base_departure_date_time DATE)")
    conn.commit()

    items = ["departures", "arrivals"]

    #2 variables permettant de vérifier par la suite que l'on se trouve bien dans la bonne journée pour mettre à jour les tables
    date_now_d = datetime.today() #Date et heure du jour
    date_now_d = date_now_d.replace(hour=0, minute=0,second=1) #On remplace l'heure actuelle par 00:00:01

    date_now_f = datetime.today() #Date et heure du jour
    date_now_f = date_now_f.replace(hour=23, minute=59,second=59) #On remplace l'heure actuelle par 23:59:59

    #On réalise la requête une fois pour les départs et une fois pour les arrivés
    for item in items:

        # generate a departures data frame
        data_raw = pd.DataFrame(data_SNCF(request_url+'/'+item+'?', item, proxy))
        if not data_raw['headsign'].empty:
            #La colonne headsign est convertie en numeric
            data_raw['headsign'] =  pd.to_numeric(data_raw['headsign'])
            #Les colonnes comportant des dates et heures sont converties en datetime
            data_raw['arrival_date_time'] =  pd.to_datetime(data_raw['arrival_date_time'],"%Y%m%dT%H%M%S")
            data_raw['base_arrival_date_time'] =  pd.to_datetime(data_raw['base_arrival_date_time'],"%Y%m%dT%H%M%S")
            data_raw['departure_date_time'] =  pd.to_datetime(data_raw['departure_date_time'],"%Y%m%dT%H%M%S")
            data_raw['base_departure_date_time'] =  pd.to_datetime(data_raw['base_departure_date_time'],"%Y%m%dT%H%M%S")

            #On lit ligne par ligne du résultat obtenu
            for index, row in data_raw.iterrows():
                #On vérifie si le headsign a déjà été enregistré et qu'il s'agisse de la journée du jour
                cur.execute("SELECT * FROM data WHERE stop_area = ? AND type = ? AND headsign = ? AND arrival_date_time BETWEEN ? AND ?", (stop_area, item, row.headsign, date_now_d, date_now_f))
                if not cur.fetchone():
                    #Si le headsign n'a pas été enregistré alors on le rajoute
                    cur.execute("INSERT INTO data (stop_area, type, headsign, direction, commercial_mode, mission, arrival_date_time, base_arrival_date_time, departure_date_time,base_departure_date_time) VALUES (?,?,?,?,?,?,?,?,?,?)", (stop_area, item, row.headsign, row.direction, row.commercial_mode, row.mission, str(row.arrival_date_time), str(row.base_arrival_date_time), str(row.departure_date_time), str(row.base_departure_date_time)))
                else:
                    #Update rows where headsign already exist et qu'il s'agisse de la journée du jour
                    cur.execute("UPDATE data SET direction = ? WHERE stop_area = ? AND type = ? AND headsign = ? AND arrival_date_time BETWEEN ? AND ?", (row.direction, stop_area, item, row.headsign, date_now_d, date_now_f))
                    cur.execute("UPDATE data SET commercial_mode = ? WHERE stop_area = ? AND type = ? AND headsign = ? AND arrival_date_time BETWEEN ? AND ?", (row.commercial_mode, stop_area, item, row.headsign, date_now_d, date_now_f))
                    cur.execute("UPDATE data SET mission = ? WHERE stop_area = ? AND type = ? AND headsign = ? AND arrival_date_time BETWEEN ? AND ?", (row.mission, stop_area, item, row.headsign, date_now_d, date_now_f))
                    cur.execute("UPDATE data SET arrival_date_time = ? WHERE stop_area = ? AND type = ? AND headsign = ? AND arrival_date_time BETWEEN ? AND ?", (str(row.arrival_date_time), stop_area, item, row.headsign, date_now_d, date_now_f))
                    cur.execute("UPDATE data SET base_arrival_date_time = ? WHERE stop_area = ? AND type = ? AND headsign = ? AND arrival_date_time BETWEEN ? AND ?", (str(row.base_arrival_date_time), stop_area, item, row.headsign, date_now_d, date_now_f))
                    cur.execute("UPDATE data SET departure_date_time = ? WHERE stop_area = ? AND type = ? AND headsign = ? AND arrival_date_time BETWEEN ? AND ?",  (str(row.departure_date_time), stop_area, item, row.headsign, date_now_d, date_now_f))
                    cur.execute("UPDATE data SET base_departure_date_time = ? WHERE stop_area = ? AND type = ? AND headsign = ? AND arrival_date_time BETWEEN ? AND ?", (str(row.base_departure_date_time), stop_area, item, row.headsign, date_now_d, date_now_f))

                #On applique les dernières requêtes effectuées
                conn.commit()

            #Item
            if item == "departures":
                departure_arrival = "departure"
            elif item =="arrivals":
                departure_arrival = "arrival"
            
            #L'URL pour récupérer un tableau des départs / arrivées avec le numéro de la voie
            my_url = 'https://www.garesetconnexions.sncf/fr/train-times/'+gares_dim[stop_area]+'/'+departure_arrival
            
            #On récupère la page
            ua = UserAgent()
            page = s.get(my_url, proxies={"https": 'socks5://seb.amoros@gmail.com:ax44<fPe7R@'+proxy}, 
                         headers={'User-Agent': ua.random},
                         timeout=time.sleep(random.randrange(1,5)))
            
            if page.text != "":
            
                #On converti en JSON
                data = page.json()

                #On mets le JSON dans un Pandas
                scrap_raw = pd.DataFrame(data['trains'])

                #On converti la colonne heure en datetime
                scrap_raw['heure'].apply(lambda x : datetime.strptime(x, "%H:%M").strftime("%Y%m%dT%H%M%S"))
                scrap_raw['heure'] = pd.to_datetime(scrap_raw.heure)
                
                #Correction des dates si elles contiennent des heures de la journée du lendemain
                day_add = 0
                for index, row in scrap_raw.iterrows():
                    if index+1 > len(scrap_raw)-1:
                        scrap_raw.loc[index, 'heure'] = scrap_raw.loc[index, 'heure'].replace(day=scrap_raw.loc[index, 'heure'].day + day_add)
                        break
                    if scrap_raw.loc[index, 'heure'] > scrap_raw.loc[index+1, 'heure']:
                        day_add = 1
                    elif scrap_raw.loc[index, 'heure'] < scrap_raw.loc[index+1, 'heure']:
                        scrap_raw.loc[index, 'heure'] = scrap_raw.loc[index, 'heure'].replace(day=scrap_raw.loc[index, 'heure'].day + day_add)   
                
                #La colonne headsign est convertie en numeric
                scrap_raw['num'] =  pd.to_numeric(scrap_raw['num'])

                #On lit ligne par ligne du résultat obtenu
                for index, row in scrap_raw.iterrows():
                    cur.execute("UPDATE data SET platform = ? WHERE stop_area = ? AND type = ? AND headsign = ? AND arrival_date_time BETWEEN ? AND ?", (row.voie, stop_area, item, row.num, date_now_d, date_now_f))
                    cur.execute("UPDATE data SET infos = ? WHERE stop_area = ? AND type = ? AND headsign = ? AND arrival_date_time BETWEEN ? AND ?", (row.infos, stop_area, item, row.num, date_now_d, date_now_f))
                    cur.execute("UPDATE data SET retard = ? WHERE stop_area = ? AND type = ? AND headsign = ? AND arrival_date_time BETWEEN ? AND ?", (row.retard, stop_area, item, row.num, date_now_d, date_now_f))

                conn.commit()

    conn.close()


if __name__ == '__main__':
    # Debut du decompte du temps
    start_time = time.time()
    
    #Exécution du code
    main()
    
    # Affichage du temps d execution
    print("Temps d execution : %s secondes ---" % (time.time() - start_time))





