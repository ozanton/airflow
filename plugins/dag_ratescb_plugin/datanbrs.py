import requests
import pandas as pd
import io

def get_num_values(currencies):
    """ Получает коды валют для заданного списка валют (основной апи c курсом дает ошибочный код валюты)"""
    try:
        response = requests.get('https://kurs.resenje.org/api/v1/currencies')
        data = response.json()

        if not isinstance(data, dict) or "currencies" not in data:
            print("Не удалось обработать ответ сервера.")
            return {}

        rates = {}
        for currency in data['currencies']:
            code = currency['code']
            if code in currencies:
                rates[code] = currency['number']

        return rates
    except requests.exceptions.RequestException as e:
        print(f"Ошибка получения данных: {e}")
        return {}

def get_rates_nbrs(currency):
    """
      Получает данные о курсах валют для конкретной валюты.  """
    try:
        response = requests.get(f'https://kurs.resenje.org/api/v1/currencies/{currency}/rates/today')
        data = response.json()
        return data
    except requests.exceptions.RequestException as e:
        print(f"Ошибка получения данных: {e}")
        return {}

def get_currency_nbrs():
    """        Получает и обрабатывает данные о курсах валют."""

    currencies = ['EUR', 'USD', 'BAM', 'NOK', 'HUF', 'GBP', 'CZK', 'RUB']

    # получение number
    exchange_rates = get_num_values(currencies)

    data_list = []

    # данные для каждой валюты
    for currency in currencies:
        detailed_data = get_rates_nbrs(currency)
        if detailed_data:
            detailed_data['number'] = exchange_rates.get(currency)
            data_list.append(detailed_data)

    # проверка данных
    if data_list:
        df_columns = ['date', 'code', 'number', 'parity', 'exchange_buy', 'exchange_middle', 'exchange_sell']
        df = pd.DataFrame(data_list, columns=df_columns)
        print(df.to_string())
        return df
    else:
        print("Не удалось собрать данные о курсах валют.")

def get_buffer_nbrs(df):
    buffer = io.StringIO()
    df_json = df.to_json(orient='records', lines=False)
    buffer.write(df_json)
    buffer.seek(0)
    return buffer.getvalue()