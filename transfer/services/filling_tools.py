import datetime
import transfer.data.conmssqlserver.extract_data as mssql_extract_data
import transfer.data.conoracle.extract_data as oracle_extract_data

import transfer.data.conmssqlserver.insert_data as mssql_insert_data

# '192.168.0.112', 'DataPROSIS', 'sa', 'Lucerde1@'

# '192.168.0.129', '1521', service_name='GEAPROD'
# user=r'GEADBA', password='fgeuorjvne', dsn=dsn_tns

mssql_extract = mssql_extract_data.ExtractData(
    '192.168.0.112', 'DataPROSIS', 'sa', 'Lucerde1@')

oracle_extract = oracle_extract_data.ExtractData(
    '192.168.0.129', 1521, 'GEAPROD', 'GEADBA', 'fgeuorjvne')

mssql_insert = mssql_insert_data.InsertData(
    '192.168.0.112', 'DataPROSIS', 'sa', 'Lucerde1@')


def check_first_step():
    # object class connection
    return True if mssql_extract.check_data() is not None else False


# first fill
def fist_filling():
    shifts = mssql_extract.get_shifts(date=datetime.date.today())

    last_assign_time = None

    for shift in shifts:
        if shift[0] <= datetime.datetime.today() <= shift[1]:
            last_assign_time = shift[0]
            break

    first_assign_date = oracle_extract.first_lane_assignment_date()

    # get lane assignments
    assignments = oracle_extract.get_lane_assigns(
        first_assign_date[0], last_assign_time)

    # insert lane assigns
    mssql_insert.insert_lane_assigns(assignments)

    first_tran_date = oracle_extract.first_transaction_date()

    # get transactions
    transactions = oracle_extract.get_transactions(
        first_tran_date[0], last_assign_time)

    # insert transactions
    mssql_insert.insert_transactions(transactions)

    # insert fin_poste - reddition
