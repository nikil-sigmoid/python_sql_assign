# Importing packages
import pandas as pd
import psycopg2
import logging

# using logger
LOG_FORMAT = "%(levelname)s %(asctime)s - %(message)s"
logging.basicConfig(filename='/Users/nik/PycharmProjects/python_sql_assignment/log/logs.log', level=logging.DEBUG, format=LOG_FORMAT)
logger = logging.getLogger()


# to run the query
def read_query(query, conn, message):
    data = None
    try:
        cur = conn.cursor()
        cur.execute(query)
        data = cur.fetchall()
        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    finally:
        logger.info(message)
        return data


if __name__ == '__main__':

    # Creating connection to the PostgreSQL Database
    conn = psycopg2.connect(host="localhost",
                            database="python_sql_assignment",
                            user="postgres",
                            password="User@123")

    # Q1
    message = "Saving employees and their managers in emp_and_managers.xlsx file"
    query = "COPY (SELECT e.ename, e.empno, m.ename " \
            "FROM emp e " \
            "LEFT OUTER JOIN emp m ON " \
            "e.mgr = m.empno) TO '/Users/nik/PycharmProjects/python_sql_assignment/data/emp_and_managers.xlsx' DELIMITER ',' CSV HEADER; "
    read_query(query, conn, message)

    # Q2(a)
    message = "Q1(a)- Creating view for compensation calculated"
    query = "CREATE OR REPLACE VIEW v_compensation AS " \
            "SELECT empno, job, deptno, " \
            "CASE " \
            "WHEN enddate IS NULL " \
            "THEN CEIL(CAST((CURRENT_DATE - startdate) AS FLOAT)/30) " \
            "ELSE CEIL(CAST((enddate - startdate) AS FLOAT)/30) " \
            "END AS months, " \
            "CASE " \
            "WHEN enddate IS NULL " \
            "THEN CEIL(CAST((CURRENT_DATE - startdate) AS FLOAT)/30)*sal " \
            "ELSE CEIL(CAST((enddate - startdate) AS FLOAT)/30) * sal " \
            "END AS compensation " \
            "FROM jobhist;"

    read_query(query, conn, message)

    # Q2(b)
    message = "Q1(b)- Calculating total compensation for each employee"
    query = "COPY (SELECT e.empno, e.ename, d.dname, comp.total_months, comp.total_compensation " \
            "FROM emp e INNER JOIN dept d " \
            "ON e.deptno = d.deptno " \
            "INNER JOIN (SELECT empno, SUM(months) AS total_months, SUM(compensation) AS total_compensation " \
            "FROM v_compensation " \
            "GROUP BY empno) AS comp " \
            "ON e.empno = comp.empno) TO '/Users/nik/PycharmProjects/python_sql_assignment/data/empwise_total_compensation.xlsx' DELIMITER ',' CSV HEADER; "

    read_query(query, conn, message)

    # Q3(a)
    message = "Q3(a)- Creating table to import empwise_total_compensation.xlsx file"
    query = "DROP TABLE IF EXISTS total_compensation_table; " \
            "CREATE TABLE total_compensation_table( " \
            "empnum NUMERIC(4) , " \
            "ename VARCHAR(10), " \
            "dname VARCHAR(14), " \
            "total_months FLOAT, " \
            "total_compensation FLOAT )"

    read_query(query, conn, message)

    # Q3(b)
    message = "Q3(b)- Importing file empwise_total_compensation.xlsx to the table"
    query = "COPY total_compensation_table FROM '/Users/nik/PycharmProjects/python_sql_assignment/data/empwise_total_compensation.xlsx' " \
            "DELIMITER ',' " \
            "CSV HEADER; "

    read_query(query, conn, message)

    # Q4
    message = "Q4- Calculating deptwise total compensation"
    query = "COPY (SELECT d.deptno, t.dname, t.total_compensation " \
            "FROM (SELECT dname, sum(total_compensation) AS total_compensation " \
            "FROM tbl_total_compensation " \
            "GROUP BY dname) t " \
            "INNER JOIN dept d " \
            "ON t.dname = d.dname) TO '/Users/nik/PycharmProjects/python_sql_assignment/data/deptwise_total_compensation.xlsx' DELIMITER ',' CSV HEADER; "

    # Closing the connection
    conn.close()
