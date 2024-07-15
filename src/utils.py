import requests
import psycopg2
from typing import Any

companies_dict = {"Яндекс": 1740,
                  "Авито": 84585,
                  "Ozon": 2180,
                  "Т-Банк": 78638,
                  "VK": 15478,
                  "Банк ВТБ": 4181,
                  "Билайн": 4934,
                  "Aviasales.ru": 588914,
                  "Газпромнефть": 39305,
                  "Сбербанк": 1473866}


def get_companies():
    """
    Получает имя компаний и их ID из companies_dict,
    и возвращает список словарей с информацией о компаниях
    """
    data = []
    for company_name, company_id in companies_dict.items():
        company_url = f"https://hh.ru/employer/{company_id}"
        company_info = {'company_id': company_id, 'company_name': company_name, 'company_url': company_url}
        data.append(company_info)

    return data


def get_vacancies(data: list):
    """
    Получает информацию о вакансиях для компаний из companies_dict
    """
    vacancies_info = []
    for company_info in data:
        company_id = company_info['company_id']
        url = f"https://api.hh.ru/vacancies?employer_id={company_id}"
        response = requests.get(url)
        if response.status_code == 200:
            vacancies_info.extend(response.json()['items'])
        else:
            return f'Запрос не выполнен с кодом состояния: {response.status_code}'
    return vacancies_info


def create_database(database_name: str, params: dict):
    """Создание базы данных и таблиц для сохранения данных о комапаниях и вакансиях."""
    conn = psycopg2.connect(dbname="postgres", **params)
    conn.autocommit = True
    cur = conn.cursor()

    try:
        cur.execute(f"DROP DATABASE {database_name}")
    except Exception as e:
        print(f"Ошибка создания базы данных: {e}")
    finally:
        cur.execute(f"CREATE DATABASE {database_name}")

    cur.close()
    conn.close()

    conn = psycopg2.connect(dbname=database_name, **params)

    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE companies (
                company_id SERIAL PRIMARY KEY,
                company_name VARCHAR(50) NOT NULL,
                company_url TEXT
            )
        """)

    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE vacancies (
                vacancy_id SERIAL PRIMARY KEY,
                company_id INT REFERENCES companies(company_id),
                vacancy_name VARCHAR(100) NOT NULL,
                vacancy_employer_name VARCHAR(100),
                vacancy_salary_from INT, 
                vacancy_salary_to INT,
                vacancy_requirement VARCHAR(250),
                vacancy_url TEXT
            )
        """)

    conn.commit()
    conn.close()


def save_data_to_database(companies_data: list[dict[str, Any]], vacancies_data: list[dict[str, Any]],
                          database_name: str, params: dict):
    """Сохранение данных о компаниях и вакансиях в базу данных."""
    conn = psycopg2.connect(dbname=database_name, **params)

    with conn.cursor() as cur:
        for company in companies_data:
            company_id = company['company_id']
            company_name = company['company_name']
            company_url = company['company_url']
            cur.execute("""
                INSERT INTO companies (company_id, company_name, company_url) VALUES (%s, %s, %s)
            """, (company_id, company_name, company_url))

    with conn.cursor() as cur:
        for vacancy in vacancies_data:
            vacancy_id = vacancy['id']
            company_id = vacancy['employer']['id']
            vacancy_name = vacancy['name']
            vacancy_employer_name = vacancy["employer"]["name"]
            salary = vacancy["salary"]
            vacancy_salary_from = salary.get("from") if salary else None
            vacancy_salary_to = salary.get("to") if salary else None
            vacancy_requirement = vacancy["snippet"]["requirement"]
            vacancy_url = vacancy["alternate_url"]
            cur.execute("""
                INSERT INTO vacancies (vacancy_id, company_id, vacancy_name, vacancy_employer_name, vacancy_salary_from, 
                vacancy_salary_to, vacancy_requirement, vacancy_url)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (vacancy_id, company_id, vacancy_name, vacancy_employer_name, vacancy_salary_from,
                  vacancy_salary_to, vacancy_requirement, vacancy_url))

    conn.commit()
    conn.close()
