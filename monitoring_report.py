import requests
import time
import pandas as pd
from datetime import datetime, timedelta
import pytz


# API_KEYS_FNAME = "api_keys_test.xlsx"
API_KEYS_FNAME = "api_keys.xlsx"


# функция проверки доступности
def check_api_key(url_methods, park_id, api_key, date_from, date_to):

    # создаем пустой словарь, в который будет помещать результаты
    avalability = {}

    # циклом проходимся по списку методов и формируем запросы
    for method in url_methods:
        # готовим хедер с парк ID и апи ключом парка, которые переданы в виде аргументов в функцию
        headers = {
            "X-Client-ID": f"taxi/park/{park_id}",
            "X-API-Key": api_key,
            "Accept-Language": "ru",
        }

        # формируем базовый body для запросов POST
        body_api = {"limit": 1, "query": {"park": {"id": park_id}}}

        # додобавляем параметры для некоторых методов
        if method["name"] == "Заказы":
            body_api["query"]["park"]["order"] = {
                "booked_at": {"from": date_from, "to": date_to}
            }
        elif method["name"] == "Транзакции":
            # метод транзакий у меня часто выдает 429 код, хотя я его гораздо реже дергаю, чем другие методы
            # 429 - это слишком частое обращение к API
            # поэтому если проблема возникает, можно увеличить sleep, пока оставил 1
            time.sleep(1)
            body_api["query"]["park"]["transaction"] = {
                "event_at": {"from": date_from, "to": date_to}
            }

        # print(method['name'], body_api)

        # попытка осуществления запроса в блоке try
        try:
            if method["type"] == "POST":
                response = requests.post(method["URL"], headers=headers, json=body_api)
            else:
                response = requests.get(method["URL"] + park_id, headers=headers)

            # если запрос проходит успешно, то указываем 'да', иначе вносим статус код
            if response.status_code == 200:
                avalability[method["name"]] = "да"
            else:
                avalability[method["name"]] = str(response.status_code)

        except Exception as e:
            avalability[method["name"]] = "ошибка"

    return avalability


# функция получения суммы заказов
# описание метода https://fleet.taxi.yandex.ru/docs/api/reference/Orders/v1_parks_orders_list_post.html
def get_orders_sum(park_id, api_key, date_from, date_to, limit=500):
    url_method = "https://fleet-api.taxi.yandex.net/v1/parks/orders/list"
    cursor = ""
    orders = []
    ret_count = limit
    i = 0

    HEADERS_API = {
        "X-Client-ID": f"taxi/park/{park_id}",
        "X-API-Key": api_key,
        "Accept-Language": "ru",
    }

    # формируем стартовый body запроса для POST метода получения заказов
    body_api = {
        "limit": limit,
        "query": {
            "park": {
                "id": park_id,
                "order": {"booked_at": {"from": date_from, "to": date_to}},
            }
        },
    }

    while ret_count == limit:
        try:
            res = requests.post(url_method, headers=HEADERS_API, json=body_api)
            # print(str(i) + ' <get orders> status code: ' + str(res.status_code))
            print(str(res.status_code), end=" ")
            i += 1
            if res.status_code == 200:
                cursor = res.json()["cursor"]
                orders += res.json()["orders"]
                ret_count = len(res.json()["orders"])
                body_api = {
                    "limit": limit,
                    "cursor": cursor,
                    "query": {
                        "park": {
                            "id": park_id,
                            "order": {"booked_at": {"from": date_from, "to": date_to}},
                        }
                    },
                }
            else:
                return 0

        except Exception as e:
            print("В процессе API запросов произошла ошибка", str(e))
            return 0

    df_ord = pd.DataFrame(orders)
    df_ord["price"] = pd.to_numeric(df_ord["price"], errors="coerce")

    ord_sum = df_ord["price"].sum()

    print("\nВсего загружено заказов: " + str(len(orders)), " на сумму =", ord_sum)
    return ord_sum


# функция создания 4 значений дата+время в формате ISO 8601
# арегумент функции - это дата последнего дня двухнедельного периода
def create_week(date_end):
    # Устанавливаем временную зону для смещения (пока не используем)
    # tz = pytz.timezone('Asia/Yekaterinburg')
    # tz = pytz.timezone("Europe/Moscow")

    # формируем крайний срок получения данных - это "вчера 23:59:59"
    # Заменяем время на конец дня
    week_end = date_end.replace(hour=23, minute=59, second=59, microsecond=999999)
    week_begin = (week_end - timedelta(days=6)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    # формируем даты предшествующей недели
    week_minus1_end = week_end - timedelta(days=7)
    week1_minus1_begin = week_begin - timedelta(days=7)

    time_dz = [week1_minus1_begin, week_minus1_end, week_begin, week_end]

    week_dates = [date.strftime("%Y-%m-%dT%H:%M:%S%z") for date in time_dz]
    print("Анализируемые даты: ", week_dates)
    return week_dates


# функция возвращает список словарей с данными по паркам (обычно это один парк)
# словарь состоит из 3 ключ-значение (city, id, name)
def get_name_and_city(park_id, api_key):
    url = "https://fleet-api.taxi.yandex.net/v1/parks/driver-profiles/list"
    # готовим хедер с парк ID и апи ключом парка, которые переданы в виде аргументов в функцию
    headers = {
        "X-Client-ID": f"taxi/park/{park_id}",
        "X-API-Key": api_key,
        "Accept-Language": "ru",
    }

    # формируем базовый body для запросов POST
    body_api = {"limit": 1, "query": {"park": {"id": park_id}}}

    # попытка осуществления запроса в блоке try
    try:
        response = requests.post(url, headers=headers, json=body_api)

        # если запрос проходит успешно, то указываем 'да', иначе вносим статус код
        if response.status_code == 200:
            return res.json()["parks"]
        else:
            return "error"

    except Exception as e:
        return "error"


if __name__ == "__main__":

    # создаем даты в формате ISO 8601, которые требует API яндекса
    # пока не заморачиваемся, собираем данные в рамках единой таймзоны
    tz = pytz.timezone("Europe/Moscow")
    week_dates = create_week(datetime.now(tz) - timedelta(days=1))

    # БЛОК 1 ПРОВЕРКА ДОСТУПНОСТИ API КЛЮЧЕЙ
    tic = time.perf_counter()
    # загружаем список клиентов (из excel файла) с их api ключами на проверку
    api_keys = pd.read_excel(f"./data/{API_KEYS_FNAME}")
    df_clients = api_keys[
        ["Наименование клиента", "ИНН", "Название кабинета", "Park ID", "API Key"]
    ].copy()
    df_clients["ИНН"] = df_clients["ИНН"].astype(str)
    print("Загружено клиентов на проверку ключей - ", df_clients.shape[0])
    print("Приступаем к проверке доступов (может занять длительное время!)")

    # создаем список методов, которые будем проверять на доступность
    url_methods = [
        {
            "name": "Список авто",
            "type": "POST",
            "URL": "https://fleet-api.taxi.yandex.net/v1/parks/cars/list",
        },
        {
            "name": "Профили водителей",
            "type": "POST",
            "URL": "https://fleet-api.taxi.yandex.net/v1/parks/driver-profiles/list",
        },
        {
            "name": "Условия работы",
            "type": "GET",
            "URL": "https://fleet-api.taxi.yandex.net/v1/parks/driver-work-rules?park_id=",
        },
        {
            "name": "Заказы",
            "type": "POST",
            "URL": "https://fleet-api.taxi.yandex.net/v1/parks/orders/list",
        },
        {
            "name": "Транзакции",
            "type": "POST",
            "URL": "https://fleet-api.taxi.yandex.net/v2/parks/transactions/list",
        },
    ]

    for _, row in df_clients.iterrows():
        res = check_api_key(
            url_methods, row["Park ID"], row["API Key"], week_dates[0], week_dates[1]
        )
        for name, value in res.items():
            df_clients.at[row.name, name] = value

    # сохраняем в файл данные по доступам
    df_clients.to_excel("./data/monitoring_report.xlsx", index=False)
    print("Проверка доступов успешено завершена!")
    toc = time.perf_counter()
    print(f"Время выполнения проверки {toc - tic:0.4f} секунд\n")

    # БЛОК 2 АНАЛИЗ ДИНАМИКИ ЗАКАЗОВ
    # проходимся циклов по датафрейму с клиентами, вызываем функцию загрузки данных и сохраняем итоги в датафрейм
    tic = time.perf_counter()
    for _, row in df_clients.iterrows():
        print("Клиент " + row["Наименование клиента"])
        wkm1 = get_orders_sum(
            row["Park ID"], row["API Key"], week_dates[0], week_dates[1]
        )
        wk = get_orders_sum(
            row["Park ID"], row["API Key"], week_dates[2], week_dates[3]
        )
        df_clients.at[row.name, "Неделю назад"] = wkm1
        df_clients.at[row.name, "Прошедшая неделя"] = wk
        if wkm1 != 0:
            df_clients.at[row.name, "Динамика в %"] = round((wk - wkm1) / wkm1 * 100, 2)
        else:
            df_clients.at[row.name, "Динамика в %"] = 0

        print("-" * 100)

    df_clients = df_clients.drop(["Park ID", "API Key"], axis=1).copy()
    df_clients.to_excel("./data/monitoring_report.xlsx", index=False)

    print(
        "Анализ заказов по клиентам завершен. Данные сохранены в файл /data/monitoring_report.xlsx"
    )

    toc = time.perf_counter()
    print(f"Время анализа заказов составило {toc - tic:0.4f} секунд\n")
