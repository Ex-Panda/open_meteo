Тестовое задание на вакансию "Инженер-программист"

Cкрипт выполняет следующие функции:
1. Запрос данных погоды через API:
* Скрипт автоматически запрашивает данные погоды в текущий момент в районе Сколтеха через заданные промежутки времени (каждые 3 минуты) и добавляет в базу данных. 
* Данные погоды включают: температуру в градусах Цельсия, направление и скорость ветра (в м/с, с указанием направления), давление воздуха в миллиметрах ртутного столба, осадки (тип и количество в мм).
2. Экспорт данных в Excel файл:
- Скрипт поддерживать команду из консоли для экспорта данных из БД в файл формата .xlsx.. Экспорт не прерывает процесс запроса данных о погоде. 
- Команда для экспорта данных *python open_meteo.py --export*


Для запросов к API используется open-meteo;

Для работы с БД используется ORM SQLAlchemy;