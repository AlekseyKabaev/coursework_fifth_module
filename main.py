from src.utils import create_database, save_data_to_database, get_companies, get_vacancies
from src.config import config
from src.dbmanager import DBManager


def main():
    params = config()
    company_list = get_companies()
    vacancy_list = get_vacancies(company_list)

    create_database('vacancies_hh', params)
    save_data_to_database(company_list, vacancy_list, 'vacancies_hh', params)

    dbm = DBManager('vacancies_hh', params)

    print(dbm.get_companies_and_vacancies_count())
    print(dbm.get_all_vacancies())
    print(dbm.get_avg_salary())
    print(dbm.get_vacancies_with_higher_salary())
    print(dbm.get_vacancies_with_keyword('менедж'))


if __name__ == '__main__':
    main()
