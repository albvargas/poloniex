#!/usr/bin/python
# -*- coding: utf-8 -*-

import json
from google.cloud import datastore
import pandas as pd
from pandas.io.json import json_normalize
import datetime
import time
import sys
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


json_cfg=open("/poloniex/config/poloniex.cfg").read()
config=json.loads(json_cfg)

project_cloud=config['project_cloud']
key_ds_cloud=config['key_ds_cloud']
url_currencies=config['url_currencies']
url_exchanges=config['url_exchanges']
conf_limit=int(config['conf_limit'])


def ReadApiPoloniex (url):
  resp_ok=False
  retries=0

  while not resp_ok and retries < 10:
    retries=retries + 1
    headers={ "cache-control" : "no-cache" 
              , "Connection" : "close"}
    resp_api=requests.request("GET", url, headers=headers, verify=False, timeout=45)

    if resp_api.status_code == 200:
       resp_ok=True
    elif resp_api.status_code == 429 or resp_api.status_code == 500: 
       print(str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))+' -> High level of calls to Aemet API. Waiting 30 seconds and retry.')
       time.sleep(30)
    else:
       print(resp_api.status_code)
   
  resp_api.encoding='latin1'      
  return resp_api


def TrueOrFalse (p_value):
    if p_value==0:
        return False
    else:
        return True


def ValidCurrency (p_delisted, p_disabled, p_frozen, p_minConf ):
    global conf_limit    
    if p_delisted==True or p_disabled==True or p_frozen==True or p_minConf>conf_limit:
        return False
    else: 
        return True



def LoadExchanges(p_exchanges):
    df_exch_aux=pd.DataFrame()
    for i_exchange in p_exchanges:
        if int(p_exchanges[i_exchange]['isFrozen'])==0:
            df_exch_aux=df_exch_aux.append( { 'currency_cod_from': str(i_exchange.split('_')[1])
                                            , 'currency_cod_to': str(i_exchange.split('_')[0])
                                            , 'last': p_exchanges[i_exchange]['last']
                                            , 'percentChange': p_exchanges[i_exchange]['percentChange']
                                            #, 'low24hr': p_exchanges[i_exchange]['low24hr']
                                            #, 'high24hr': p_exchanges[i_exchange]['high24hr']
                                            ,  } , ignore_index=True )

            df_exch_aux=df_exch_aux.append( { 'currency_cod_from': i_exchange.split('_')[0]
                                            , 'currency_cod_to':i_exchange.split('_')[1]
                                            , 'last': (1/float(p_exchanges[i_exchange]['last']))
                                            , 'percentChange': ((1/float(p_exchanges[i_exchange]['last']))/(1/(float(p_exchanges[i_exchange]['last'])/(1+float(p_exchanges[i_exchange]['percentChange'])))))-1
                                            #, 'low24hr': float("{0:.8f}".format((1/float(p_exchanges[i_exchange]['low24hr']))))
                                            #, 'high24hr': float("{0:.8f}".format((1/float(p_exchanges[i_exchange]['high24hr']))))
                                            ,  } , ignore_index=True )

    #Quitar "aisladas"=solo una relacion
    df_curr_noalone=df_exch_aux.groupby(['currency_cod_from']).size().reset_index(name='size')
    df_curr_noalone=df_curr_noalone.rename(columns={'currency_cod_from':'currency_cod'})
    df_curr_noalone=df_curr_noalone[df_curr_noalone['size']>=2]
    del df_curr_noalone['size']

    df_exch_aux=pd.merge(df_exch_aux, df_curr_noalone[['currency_cod']], left_on=['currency_cod_from'], right_on=['currency_cod'], how='inner')
    del df_exch_aux['currency_cod']
    df_exch_aux=pd.merge(df_exch_aux, df_curr_noalone[['currency_cod']], left_on=['currency_cod_to']  , right_on=['currency_cod'], how='inner')
    del df_exch_aux['currency_cod']
    df_exch_aux['exchange_utc']=datetime.datetime.utcnow()
    df_exch_aux=df_exch_aux.reset_index()

    return df_exch_aux


#-------------------------

if __name__ == '__main__':
    print(str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + '> Inicio.')
    df_curr_exchanges=pd.DataFrame()

    r_currencies=ReadApiPoloniex(url_currencies)
    js_currencies=json.loads(r_currencies.text)

    client=datastore.Client.from_service_account_json(key_ds_cloud, project=project_cloud)
    client.namespace='Currency'
    
    print(str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + '> Carga las criptomonedas actuales.')

    for i_curr_cod in js_currencies:
        key_curr=client.key('CurrencyList', i_curr_cod)
        task=datastore.Entity(key=key_curr)

        delisted=TrueOrFalse(js_currencies[i_curr_cod]['delisted'])
        disabled=TrueOrFalse(js_currencies[i_curr_cod]['disabled'])
        frozen=TrueOrFalse(js_currencies[i_curr_cod]['frozen'])

        validcurrency=ValidCurrency(delisted, disabled, frozen, conf_limit)

        task.update({
            'currency_id': js_currencies[i_curr_cod]['id'],
            'currency_cod': i_curr_cod,
            'currency_name': js_currencies[i_curr_cod]['name'],
            'txFee': js_currencies[i_curr_cod]['txFee'],
            'minConf': js_currencies[i_curr_cod]['minConf'],
            'depositAddress': js_currencies[i_curr_cod]['depositAddress'],
            'delisted': delisted,
            'disabled': disabled,
            'frozen': frozen,
            'validcurrency': validcurrency,
            'insert_dt': datetime.datetime.now()
            })

        client.put(task)

        if validcurrency:
            #print(str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + '> Carga criptomoneda valida: '+str(i_curr_cod))
            df_curr_exchanges=df_curr_exchanges.append( { 'currency_cod_from':i_curr_cod, 'false_key':0 } , ignore_index=True )


    df_curr_dest=df_curr_exchanges
    df_curr_dest=df_curr_dest.rename(columns={'currency_cod_from':'currency_cod_to'})
    df_curr_exchanges=pd.merge(df_curr_exchanges, df_curr_dest, on=['false_key'], how='inner')
    df_curr_exchanges=df_curr_exchanges[df_curr_exchanges.currency_cod_from!=df_curr_exchanges.currency_cod_to] 
    df_curr_exchanges=df_curr_exchanges.drop('false_key', axis=1)

    print(str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + '> Selecciona los tipos de cambio con al menos 2 monedas a las que poder cambiar.')
    r_exchanges=ReadApiPoloniex(url_exchanges)
    js_exchanges=json.loads(r_exchanges.text)
    df_exchanges=LoadExchanges(js_exchanges)

    for i_exch in range(int(len(df_exchanges.index))):
        key_exch=client.key('Exchange')
        task=datastore.Entity(key=key_exch)

        task.update({
            'exchange_dt_utc': df_exchanges['exchange_utc'][i_exch],
            'currency_cod_from': df_exchanges['currency_cod_from'][i_exch],
            'currency_cod_to': df_exchanges['currency_cod_to'][i_exch],
            'last': float(df_exchanges['last'][i_exch]),
            'percentChangeLast24h': float(df_exchanges['percentChange'][i_exch]),
            'insert_dt': datetime.datetime.now()
            })

        client.put(task)


    print(str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + '> Fin.')

