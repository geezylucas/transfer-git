from datetime import datetime, timedelta
from transfer.connections.conmssqlserver.connection import ConnMSSQLServer


class ExtractData(ConnMSSQLServer):

    def __init__(self, server, database, user, password):
        ConnMSSQLServer.__init__(self, server, database, user, password)

    def check_data(self):
        cursor = self.execute_query('select top (1) MSG_Date from dbo.AsignacionesCarril order by MSG_Date asc')
        return cursor.fetchone()

    def get_shifts(self, date=None):
        cursor = self.execute_query('select TurnoId, NombreTurno, HoraInicio, HoraFin from dbo.Turnos')
        shifts_db = [(shift[2], shift[3], shift[1]) for shift in cursor.fetchall()]

        if date:
            shifts = []

            for shift in shifts_db:
                if shift[0][:2] == '22':
                    shifts.append((datetime.combine(date + timedelta(days=-1), self.__convert_time(shift[0])),
                                   (datetime.combine(date, self.__convert_time(shift[1])))))
                elif shift[0][:2] == '06' or shift[0][:2] == '14':
                    shifts.append((datetime.combine(date, self.__convert_time(shift[0])),
                                   datetime.combine(date, self.__convert_time(shift[1]))))

            return shifts
        else:
            return shifts_db

    @staticmethod
    def __convert_time(time):
        return datetime.strptime(str(time), '%H:%M:%S').time()
