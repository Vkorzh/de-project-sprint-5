import requests

class CouriersReader:
    def __init__(self, nickname, cohort, api_key):
        """
        Инициализация класса с параметрами для заголовков запроса.
        
        :param nickname: Ваш никнейм (X-Nickname).
        :param cohort: Номер вашей когорты (X-Cohort).
        :param api_key: Ваш API-ключ (X-API-KEY).
        """
        self.base_url = "https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/couriers"
        self.headers = {
            "X-Nickname": nickname,
            "X-Cohort": str(cohort),
            "X-API-KEY": api_key
        }

    def get_couriers(self, sort_field="id", sort_direction="asc", limit=50, offset=0):
        """
        Получение списка курьеров из API.

        :param sort_field: Поле для сортировки (id или name).
        :param sort_direction: Направление сортировки (asc или desc).
        :param limit: Максимальное количество записей (от 0 до 50).
        :param offset: Смещение для пагинации.
        :return: Список курьеров в формате JSON.
        """
        # Параметры запроса
        params = {
            "sort_field": sort_field,
            "sort_direction": sort_direction,
            "limit": limit,
            "offset": offset
        }

        # Выполнение GET-запроса
        response = requests.get(
            self.base_url,
            headers=self.headers,
            params=params
        )

        # Проверка статуса ответа
        if response.status_code == 200:
            return response.json()  # Возвращаем JSON-ответ
        else:
            # В случае ошибки возвращаем сообщение об ошибке
            return {"error": f"Ошибка {response.status_code}: {response.text}"}

    def get_all_couriers(self, sort_field="id", sort_direction="asc", limit=50):
        """
        Получение всех курьеров с использованием пагинации.

        :param sort_field: Поле для сортировки (id или name).
        :param sort_direction: Направление сортировки (asc или desc).
        :param limit: Максимальное количество записей за один запрос (от 0 до 50).
        :return: Список всех курьеров в формате JSON.
        """
        all_couriers = []  # Список для хранения всех курьеров
        offset = 0  # Начальное смещение

        while True:
            # Получаем данные с текущим offset
            couriers = self.get_couriers(sort_field, sort_direction, limit, offset)

            # Если произошла ошибка, возвращаем её
            if "error" in couriers:
                return couriers

            # Если данных больше нет, выходим из цикла
            if not couriers:
                break

            # Добавляем полученные данные в общий список
            all_couriers.extend(couriers)

            # Если количество полученных данных меньше limit, значит, это последняя страница
            if len(couriers) < limit:
                break

            # Увеличиваем offset для следующего запроса
            offset += limit

        return all_couriers

