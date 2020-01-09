from transfer.connections.conoracle.connection import ConnOracle


class ExtractData(ConnOracle):

    def __init__(self, server, port, service_name, user, password):
        ConnOracle.__init__(self, server, port, service_name, user, password)

    def first_lane_assignment_date(self):
        cursor = self.execute_query('select x.* '
                                    'from (select MSG_DHM '
                                    '       from LANE_ASSIGN '
                                    '       order by MSG_DHM asc) x '
                                    'where rownum = 1')
        return cursor.fetchone()

    def first_transaction_date(self):
        cursor = self.execute_query('select x.*'
                                    'from (select DATE_TRANSACTION'
                                    '       from TRANSACTION '
                                    '       order by DATE_TRANSACTION asc) x '
                                    'where rownum = 1')

        return cursor.fetchone()

    def get_lane_assigns(self, start, end):
        cursor = self.execute_query(f"""
                                    select
                                        ID_PLAZA ,              --IdGare
                                        ID_LANE ,               --LineaCarril
                                        SHIFT_NUMBER ,          --TurnoId
                                        OPERATION_ID ,          --OperacionId
                                        MSG_DHM ,               --MSG_Date
                                        ASSIGN_DHM ,            --Assign_Date
                                        LANE_TYPE ,             --TipoCarril
                                        STAFF_NUMBER ,          --NumeroGeaOperador
                                        JOB_NUMBER ,            --JobNumber
                                        IN_CHARGE_SHIFT_NUMBER  --NumEncargado
                                    from LANE_ASSIGN
                                    where MSG_DHM between 
                                    TO_DATE('{start.strftime('%d-%m-%Y %H:%M:%S')}', 'dd-mm-yyyy hh24:mi:ss') 
                                    and TO_DATE('{end.strftime('%d-%m-%Y %H:%M:%S')}', 'dd-mm-yyyy hh24:mi:ss') 
                                    order by MSG_DHM asc""")

        return cursor.fetchall()

    def get_transactions(self, start, end):
        cursor = self.execute_query(f"""
               select 
                    A.EVENT_NUMBER,                                                 --numeroevento
                    A.ID_GARE,                                                      --idgare
                    A.SHIFT_NUMBER,                                                 --turnoid
                    A.VOIE,                                                         --lineacarril
                    A.ID_PAIEMENT,                                                  --tipopagoid
                    A.MODE_REGLEMENT,                                               --tipopagodesc
                    A.MATRICULE,                                                    --numerogea
                    to_char(A.DATE_DEBUT_POSTE, 'YYYY-MM-DD') as FECHAINIC,         --fechaaperturaturno
                    to_char(A.DATE_DEBUT_POSTE,'HH24:MI:SS') as HORAINIC,           --horainicioturno
                    to_char(A.DATE_TRANSACTION, 'YYYY-MM-DD') as FECHA,             --fecha
                    to_char(A.DATE_TRANSACTION,'HH24:MI:SS') as HORA,               --hora
                    A.FOLIO_ECT,                                                    --folio
                    A.INDICE_SUITE,                                                 --indicereclasificacion
                    A.ID_CLASSE,                                                    --pre
                    A.TAB_ID_CLASSE,                                                --cajero
                    A.ACD_CLASS,                                                    --post
                    A.ID_OBS_TT,                                                    --observacionestt
                    A.ID_OBS_MP,                                                    --observacionesmp
                    A.ID_OBS_SEQUENCE,                                              --observacionessecuencia
                    A.ID_OBS_PASSAGE,                                               --observacionespaso
                    A.CODE_GRILLE_TARIF,                                            --numeroejes
                    0 as anualado,                                                  --anualado
                    '' as comentarios,                                              --comentarios
                    0 as capturado,                                                 --capturado
                    A.CONTENU_ISO,                                                  --numiave
                    A.ID_MODE_VOIE,                                                 --carrilmodoid
                    B.CHEMIN_FICHIER as URL_IMAGEN,                                 --url_imagen
                    B.INFORMATION as PLACA,                                         --placa
                    to_char(B.date_image, 'YYYY-MM-DD HH24:MI:SS') as FECHA_IMAGEN, --fechaimagen
                    A.PRIX_TOTAL                                                    --prixtotal
                from TRANSACTION A left join IMG_VIDEO_IMAGE B on 
                (A.VOIE = B.VOIE and A.ID_GARE = B.ID_GARE and
                to_char(A.DATE_TRANSACTION, 'YYYY-MM-DD HH24:MI:SS') = to_char(B.DATE_IMAGE, 'YYYY-MM-DD HH24:MI:SS'))
                where DATE_TRANSACTION
                between to_date('{start.strftime('%d-%m-%Y %H:%M:%S')}', 'dd-mm-yyyy hh24:mi:ss') 
                and to_date('{end.strftime('%d-%m-%Y %H:%M:%S')}', 'dd-mm-yyyy hh24:mi:ss')
                order by FECHA asc """)

        return cursor.fetchall()
