import requests
from logging import Logger
from datetime import datetime, timedelta

class DeliveryReader:
    def __init__(self, nickname, cohort, api_key, logger: Logger):
        """
        Инициализация класса с параметрами для заголовков запроса.
        
        :param nickname: Ваш никнейм (X-Nickname).
        :param cohort: Номер вашей когорты (X-Cohort).
        :param api_key: Ваш API-ключ (X-API-KEY).
        """
        self.base_url = "https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/deliveries"
        self.headers = {
            "X-Nickname": nickname,
            "X-Cohort": str(cohort),
            "X-API-KEY": api_key
        }
        self.log = logger

    def get_deliveries(self, sort_field="date", sort_direction="asc", limit=50, From_dt=None, restaurant_id=None, to_dt=None):
        """
        Получение списка доставок из API с параметрами фильтрации.

        :param sort_field: Поле для сортировки (id или name).
        :param sort_direction: Направление сортировки (asc или desc).
        :param limit: Максимальное количество записей (от 0 до 50).
        :param From_dt: Начальная дата для фильтрации.
        :param restaurant_id: ID ресторана для фильтрации.
        :param to_dt: Конечная дата для фильтрации.
        :return: Список курьеров в формате JSON.
        """
        # Параметры запроса
        params = {
            "sort_field": sort_field,
            "sort_direction": sort_direction,
            "limit": limit,
        }

        # Добавляем фильтры
        if From_dt:
            params["from"] = From_dt  # Используем маленькие буквы для параметра
            self.log.info(f"Загружаем от: {From_dt}")
        
        if to_dt:
            params["to"] = to_dt

        if restaurant_id:
            params["restaurant_id"] = restaurant_id

        # Выполнение GET-запроса
        response = requests.get(
            self.base_url,
            headers=self.headers,
            params=params
        )

        # Проверка статуса ответа
        if response.status_code == 200:
            self.log.info(f"Загружено успешно. Ответ: {response.json()}")
            return response.json()  # Возвращаем JSON-ответ
        else:
            # В случае ошибки возвращаем сообщение об ошибке
            return {"error": f"Ошибка {response.status_code}: {response.text}"}

    def get_all_deliveries(self, sort_field="date", sort_direction="asc", limit=50, From=None, restaurant_id=None, to=None):
        """
        Получение всех доставок с использованием пагинации.

        :param sort_field: Поле для сортировки.
        :param sort_direction: Направление сортировки.
        :param limit: Максимальное количество записей.
        :param From: Начальная дата для фильтрации.
        :param restaurant_id: ID ресторана для фильтрации.
        :param to: Конечная дата для фильтрации.
        :return: Список всех доставок.
        """
        all_deliveries = []  # Список для хранения всех доставок
        
        if From is None:
            From = datetime(2022, 1, 1).isoformat()

        while True:
            self.log.info(f"Подаем на загрузку от: {From}")
            deliveries = self.get_deliveries(sort_field, sort_direction, limit, From_dt=From, restaurant_id=restaurant_id, to_dt=to)

            if "error" in deliveries:
                return deliveries

            if deliveries:
                formatted_date = []
                for delivery in deliveries:
                    iso_date_str = delivery["delivery_ts"]
                    dt = datetime.fromisoformat(iso_date_str)
                    dt += timedelta(seconds=1)
                    formatted_date.append(dt.strftime('%Y-%m-%d %H:%M:%S'))

                if formatted_date:
                    From = max(formatted_date)
                    self.log.info(f"Список дат: {From}")

            # Если данных больше нет, выходим из цикла
            if not deliveries or len(deliveries) < limit:
                self.log.info("Выход из цикла")
                break

            # Добавляем полученные данные в общий список
            all_deliveries.extend(deliveries)
            self.log.info(f"Длина списка: {len(all_deliveries)}")

        return all_deliveries
