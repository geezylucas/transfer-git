import transfer.services.filling_tools as mssql_tools


def run():
    if not mssql_tools.check_first_step():
        mssql_tools.fist_filling()
    else:
        mssql_tools.fist_filling()
        print('The database does have data')
