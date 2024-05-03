from pycbrf import ExchangeRates
import datetime
from datetime import timedelta
import pandas as pd
import io

def get_rates_cbrf():
    #  дата: запуск ежедневно для now - 9.00 утра и tomorrow -  15.00 дня
    date_str = str(datetime.datetime.now())[:10]
    tomorrow_str = (datetime.datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
    # запрос
    rates = ExchangeRates(date_str, locale_en=True)
    # список валют
    currencies = ['USD', 'EUR', 'RSD', 'BAM', 'AMD', 'GEL', 'KZT']
    data = {
        'date': date_str,
        'name': [],
        'code': [],
        'num': [],
        'rate': [],
    }

    for code in currencies:
        # курс по коду валюты
        rate = rates[code]

        # наличие данных
        if rate:
            data['name'].append(rate.name)
            data['code'].append(rate.code)
            data['num'].append(rate.num)
            data['rate'].append(rate.rate)

    df = pd.DataFrame(data)

    print(df.to_string())
    return df

def get_buffer_cbrf(df):
    buffer = io.StringIO()
    df_json = df.to_json(orient='records', lines=False)
    buffer.write(df_json)
    buffer.seek(0)
    return buffer.getvalue()
