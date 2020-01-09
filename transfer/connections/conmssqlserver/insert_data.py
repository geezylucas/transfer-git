from transfer.connections.conmssqlserver.connection import ConnMSSQLServer


class InsertData(ConnMSSQLServer):

    def __init__(self, server, database, user, password):
        ConnMSSQLServer.__init__(self, server, database, user, password)

    def insert_lane_assigns(self, assignments):
        self.insert_many('{CALL dbo.sp_insertasignacioncarril(?,?,?,?,?,?,?,?,?,?)}', assignments)

    def insert_transactions(self, transactions):
        self.insert_many('{CALL dbo.sp_inserttransaccion(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)}',
                         transactions)
