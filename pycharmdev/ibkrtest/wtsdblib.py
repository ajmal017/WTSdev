import psycopg2


class wtsdbconn:
    def newconnection(database):
        if (database == 'WTS'):
            return (None)
        elif (database == 'WTSDEV'):
            return (psycopg2.connect(user="postgres",
                             password="Pas2021!",
                             host="172.17.0.2",
                             port="5432",
                             database="wtsdev"))
        else:
            return (None)


