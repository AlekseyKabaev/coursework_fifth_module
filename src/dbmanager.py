import psycopg2
from src.config import config


class DBManager:
    """
    Класс для взаимодействия с БД
    """

    def __init__(self, database_name, params=config()):
        self.database_name = database_name
        self.params = params

    def get_companies_and_vacancies_count(self):
        """получает список всех компаний и количество вакансий у каждой компании"""
        conn = psycopg2.connect(dbname=self.database_name, **self.params)
        with conn.cursor() as cur:
            cur.execute(
                """SELECT company_name, COUNT(vacancy_id) FROM companies 
                JOIN vacancies USING (company_id) GROUP BY company_name;""")
            companies_and_vacancies = cur.fetchall()
        conn.close()
        return companies_and_vacancies

    def get_all_vacancies(self):
        """получает список всех вакансий с указанием названия компании,
        названия вакансии и зарплаты и ссылки на вакансию"""
        conn = psycopg2.connect(dbname=self.database_name, **self.params)
        with conn.cursor() as cur:
            cur.execute(
                """SELECT company_name, vacancy_name, vacancy_salary_from, 
                vacancy_salary_to, vacancy_url FROM vacancies JOIN companies USING (company_id);""")
            all_vacancies = cur.fetchall()
        conn.close()
        return all_vacancies

    def get_avg_salary(self):
        """получает среднюю зарплату по вакансиям"""
        conn = psycopg2.connect(dbname=self.database_name, **self.params)
        with conn.cursor() as cur:
            cur.execute("""SELECT AVG(vacancy_salary_from) AS average_salary FROM vacancies;""")
            avg_salary = round(cur.fetchone()[0], 3)
        conn.close()
        return avg_salary

    def get_vacancies_with_higher_salary(self):
        """получает список всех вакансий, у которых зарплата выше средней по всем вакансиям"""
        conn = psycopg2.connect(dbname=self.database_name, **self.params)
        with conn.cursor() as cur:
            cur.execute("""SELECT * FROM vacancies 
            WHERE vacancy_salary_from > (SELECT AVG(vacancy_salary_from) FROM vacancies);""")
            higher_salary_vacancies = cur.fetchall()
        conn.close()
        return higher_salary_vacancies

    def get_vacancies_with_keyword(self, keyword):
        """получает список всех вакансий, в названии которых содержатся переданные в метод слова"""
        conn = psycopg2.connect(dbname=self.database_name, **self.params)
        with conn.cursor() as cur:
            cur.execute("""SELECT * FROM vacancies WHERE lower(vacancy_name) ILIKE %s""", ('%' + keyword + '%',))
            keyword_vacancies = cur.fetchall()
        conn.close()
        return keyword_vacancies
