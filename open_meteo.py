import asyncio
import logging
import aiohttp
import click
import pytz
from sqlalchemy import Column, Integer, Float, String, DateTime, select
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from openpyxl import Workbook


logging.basicConfig(level=logging.INFO)
moscow_tz = pytz.timezone('Europe/Moscow')
Base = declarative_base()


class WeatherData(Base):
    __tablename__ = 'weather_data'

    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, default=datetime.now)
    temperature = Column(Float)
    precipitation_type = Column(String)
    precipitation_amount = Column(Float)
    pressure = Column(Float)
    wind_speed = Column(Float)
    wind_direction = Column(String)


# URL для подключения к базе данных
DATABASE_URL = "sqlite+aiosqlite:///weather.db"
engine = create_async_engine(DATABASE_URL)
async_session = sessionmaker(engine, class_=AsyncSession)


# Определение направления ветра по углу
def get_wind_direction(angle):
    directions = ['С', 'ССВ', 'СВ', 'ВСВ', 'В', 'ВЮВ', 'ЮВ', 'ЮЮВ', 'Ю', 'ЮЮЗ', 'ЮЗ', 'ЗЮЗ', 'З', 'ЗСЗ', 'СЗ', 'ССЗ']
    index = round(angle / (360. / len(directions))) % len(directions)
    return directions[index]


# Получения данных о погоде
async def fetch_weather_data():

    current_time_msk = datetime.now(moscow_tz)
    logging.info(f"{current_time_msk}: Получаю данные о погоде")

    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": "55.6878",
        "longitude": "37.3684",
        "current": "temperature_2m,rain,showers,snowfall,pressure_msl,wind_speed_10m,wind_direction_10m",
        "wind_speed_unit": "ms"
    }

    # Получение данных о погоде
    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params) as response:
            data = await response.json()

    snowfall_mm = data["current"]["snowfall"] * 10  # конвертация из см в мм
    wind_direction = get_wind_direction(data["current"]["wind_direction_10m"])

    # Определение типа и количества осадков
    precipitation_type = None
    precipitation_amount = None
    if data["current"]["rain"] > 0:
        precipitation_type = "rain"
        precipitation_amount = data["current"]["rain"]
    elif data["current"]["showers"] > 0:
        precipitation_type = "showers"
        precipitation_amount = data["current"]["showers"]
    elif data["current"]["snowfall"] > 0:
        precipitation_type = "snowfall"
        precipitation_amount = snowfall_mm
    else:
        precipitation_type = "no precipitation"
        precipitation_amount = 0

    # Запись полученных данных о погоде в базу данных
    async with async_session() as session:
        weather_data = WeatherData(
            temperature=data["current"]["temperature_2m"],
            precipitation_type=precipitation_type,
            precipitation_amount=precipitation_amount,
            pressure=data["current"]["pressure_msl"],
            wind_speed=data["current"]["wind_speed_10m"],
            wind_direction=wind_direction,
        )
        session.add(weather_data)
        await session.commit()


# Экспорт данных о погоде в файл .xlsx
async def export_to_excel():
    # Создание нового Excel-файла и добавление данных
    workbook = Workbook()
    sheet = workbook.active
    sheet.append(["Timestamp", "Temperature (°C)", "Precipitation Type", "Precipitation Amount (mm)", "Pressure (hPa)",
                  "Wind Speed (m/s)", "Wind Direction"])

    # Получение последних 10 записей из базы данных и запись их в Excel-файл
    async with async_session() as session:
        async with session.begin():
            weather_data = await session.execute(select(WeatherData).order_by(WeatherData.timestamp.desc()).limit(10))
            for data in weather_data.scalars():
                sheet.append([data.timestamp, data.temperature, data.precipitation_type, data.precipitation_amount,
                              data.pressure, data.wind_speed, data.wind_direction])

    workbook.save("weather_data.xlsx")
    logging.info("Данные успешно экспортированы в файл weather_data.xlsx")


# Создания таблицы в базе данных
async def create_table():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


# Основная функция
@click.command()
@click.option('--export', is_flag=True, help='Export data to Excel')
def main(export):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(create_table())
    if export:
        loop.run_until_complete(export_to_excel())
    else:
        loop.run_until_complete(run_forever())


# Получения данных о погоде
async def run_forever():
    while True:
        await fetch_weather_data()
        await asyncio.sleep(180)  # каждые 3 минуты

if __name__ == "__main__":
    main()
