import psycopg2


class wtsdbconn:
    def newconnection(system):
        if (system == 'WTS'):
            return (None)
        elif (system == 'WTSDEV'):
            return (psycopg2.connect(user="postgres",
                             password="Pas2021!",
                             host="172.17.0.2",
                             port="5432",
                             database="wtsdev"))
        else:
            return (None)


