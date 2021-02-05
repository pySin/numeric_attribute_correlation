# Masure To Class
# Measures and ID are needed only.

import mysql.connector
import re


class m_to_class:

    def __init__(self, table_name):
        self.table_name = table_name

    def get_col_names(self):
        # Get the table columns names. A string has to be constructed that
        # looks exactly like SQL query. This query is then sent to the MySQL
        # server and the result is assigned to a variable with fetchall()
        # function.

        self.database = self.table_name.split('.')[0]  # Obtain database and
        self.table = self.table_name.split('.')[1]  # table names.

        get_c_names = '''
                  SELECT COLUMN_NAME
                  FROM INFORMATION_SCHEMA.COLUMNS
                  WHERE TABLE_SCHEMA = \'%s\'
                  AND
                  TABLE_NAME = \'%s\';
                      ''' % (self.database, self.table)

        conn = mysql.connector.connect(host='localhost', user='root',
                                       password='dance')  # MySQL connection.
        cursor = conn.cursor()
        cursor.execute(get_c_names)
        col_names = cursor.fetchall()

        # Transform the list from the fetchall() function to simple
        # list of strings.
        self.table_columns = [re.sub(r'\(|\)|\'|,', '', str(x))
                              for x in col_names]
        # self.table_columns.remove('BCL2_N')

        # Create self.id variable to be used in INNER JOIN later(20012021).
        self.id = self.table_columns[self.table_columns.index('id')]

        conn.commit()  # Send the query to MySQL.

    def copy_main_table(self):
        # Make 2 copies of the main table. The second copy will be transformed
        # to have values from 1 to 4. These values represent in which quarter
        # of the column range a particular cell was found to be. The firs
        # column is used for the calculations while the second column is
        # changing.

        table_copy = '''
                CREATE TABLE IF NOT EXISTS
                    %s_copy AS
                SELECT * FROM %s;
                     ''' % (self.table_name, self.table_name)

        table_copy_2 = '''
                CREATE TABLE IF NOT EXISTS
                    %s_copy_2 AS
                SELECT * FROM %s;
                     ''' % (self.table_name, self.table_name)

        conn = mysql.connector.connect(host='localhost', user='root',
                                       password='dance')
        cursor = conn.cursor()
        cursor.execute(table_copy)
        cursor.execute(table_copy_2)

        conn.commit()  # Send the query to MySQL.
        conn.close()

    def class_values(self, column_name):
        # Transfom all the values to their respective ranges in the
        # fluctuation range from 1 to 4 on 'copy_2' table. The values are
        # changed column by column.
        # Warning: a second run changes the values from 1 to 4 to different
        # values! Ths script shpuld be run only once for a single measures
        # table.

        # This calculation is tested and is working good!
        change_values = '''
            UPDATE %s_copy_2

                SET %s = (CASE
                WHEN %s BETWEEN (SELECT MIN(%s)
                FROM %s_copy)
                AND (SELECT MIN(%s) + ((MAX(%s) - MIN(%s))/4)
                FROM %s_copy)
                THEN 1

                WHEN %s BETWEEN (SELECT MIN(%s)
                + ((MAX(%s) - MIN(%s))/4)
                FROM %s_copy)
                AND (SELECT MIN(%s)
                + (((MAX(%s) - MIN(%s))/4)*2)
                FROM %s_copy)
                THEN 2

                WHEN %s BETWEEN (SELECT MIN(%s)
                + (((MAX(%s) - MIN(%s))/4)*2)
                FROM %s_copy)
                AND (SELECT MIN(%s)
                + (((MAX(%s) - MIN(%s))/4)*3)
                FROM %s_copy)
                THEN 3

                WHEN %s BETWEEN (SELECT MIN(%s)
                + (((MAX(%s) - MIN(%s))/4)*3)
                FROM %s_copy)
                AND(SELECT MIN(%s)
                + (((MAX(%s) - MIN(%s))/4)*4)
                FROM %s_copy)
                THEN 4
                END);
                ''' % (self.table_name, column_name, column_name, column_name,
                       self.table_name, column_name, column_name, column_name,
                       self.table_name,  # THEN 1

                       column_name, column_name, column_name, column_name,
                       self.table_name, column_name, column_name,
                       column_name, self.table_name,  # THEN 2

                       column_name, column_name, column_name, column_name,
                       self.table_name,
                       column_name, column_name, column_name,
                       self.table_name,  # THEN 3

                       column_name, column_name, column_name, column_name,
                       self.table_name, column_name, column_name, column_name,
                       self.table_name)  # THEN 4

        conn = mysql.connector.connect(host='localhost', user='root',
                                       password='dance')
        cursor = conn.cursor()
        cursor.execute(change_values)
        conn.commit()  # Send the query to MySQL.
        conn.close()

    def range_avg(self, main_column, secondary_columns):
        # Create 78 new tables. 1 table for each measure column. Each table
        # should have 1 main Column and 77 secondary. The main column has 4
        # numbers(1-4) for the 4 quarters of the column's range. The other
        # columns represent their own average GROUPED BY the Main Column. The
        # main column's quarters are taken from the second '...copy_2' table
        # with JOIN.

        table = self.table_name.split('.')[1]

        # Remove the main column from the secondary columns. This is done for
        # each loop with the function.
        sec_columns = secondary_columns[:]
        sec_columns.remove(main_column)

        # Create the inner columns expression. All 78 lines.
        sub_cols = ''
        for sub_protein in sec_columns:
            sub_cols = sub_cols + \
                       'AVG(%s_copy.%s) AS \'AVG_%s\',\n' % (table,
                                                             sub_protein,
                                                             sub_protein)
        sub_cols = sub_cols[:-2]  # Remove the last comma from the expression.

        # Put all the variables in a full MySQL expression. / Find main column
        # range paralels in all sub-columns.
        range_calc = '''
            CREATE TABLE IF NOT EXISTS %s_%s AS
            SELECT %s_copy_2.%s,
            %s
            FROM %s_copy
            JOIN %s_copy_2
            ON %s_copy.%s = %s_copy_2.%s
            GROUP BY %s_copy_2.%s
            ORDER BY %s_copy_2.%s ASC;
                     ''' % (self.table_name, main_column, table, main_column,
                            sub_cols, self.table_name, self.table_name, table,
                            self.id, table, self.id, table, main_column, table,
                            main_column)

        conn = mysql.connector.connect(host='localhost', user='root',
                                       password='dance')
        cursor = conn.cursor()
        cursor.execute(range_calc)
        conn.commit()
        conn.close()

    def yes_no_similarity(self):
        # Create similarity 'Yes-No' table. The final table. In this table
        # we'll check if a protein influences another protein for all proteins.

        columns_with_dtypes = self.table_columns[:]
        columns_with_dtypes.remove('id')

        # Create 78 lines with protein names and their extensions.
        query_part = ''
        for item in columns_with_dtypes:
            query_part = query_part + (item+' VARCHAR(50),\n')

        # Add the 78 lines to this expression and create the new table.
        yes_no = '''
            CREATE TABLE %s_cs(
            id INT NOT NULL AUTO_INCREMENT,
            %s_c VARCHAR(150),
            %s
            PRIMARY KEY(id)
            );
                 ''' % (self.table_name, self.database, query_part)

        conn = mysql.connector.connect(host='localhost', user='root',
                                       password='dance')
        cursor = conn.cursor()
        cursor.execute(yes_no)
        conn.commit()
        conn.close()

    def yes_no_insert(self, main_column):
        # Check if the secondary proteins match the main protein on the level
        # of their concentration. Number 1 of the main protein should match
        # the lowest average of the secondary protein. The 2 should be smaller
        # than 3 and 3 smaller than 4. Inthis way the main and the secondary
        # protein synchronize on there concentration levels.

        # Copy the table columns and remove the main column so it doesn't
        # compare with itself.
        self.main_columns = self.table_columns[:]
        self.main_columns.remove(main_column)

        # Create MySQL INSERT query.
        query_part = ''  # The beginning of the query with the columns names.
        case_query_part = ''  # The CASE comparisson part.

        # Insert every column name and every CASE sifted value with a loop.
        for item in self.main_columns:
            # Create string with the columns names.
            query_part = query_part + item+','

            # Create a CASE expression for every column with it's 76
            # sub-columns(more then 1400 lines of MySQL code for every main
            # column).
            case_query_part = case_query_part + '''
    (SELECT (CASE
    WHEN
          (SELECT AVG_%s FROM %s_%s WHERE %s = 1)
        < (SELECT AVG_%s FROM %s_%s WHERE %s = 2)
        AND
          (SELECT AVG_%s FROM %s_%s WHERE %s = 2)
        < (SELECT AVG_%s FROM %s_%s WHERE %s = 3)
        AND
          (SELECT AVG_%s FROM %s_%s WHERE %s = 3)
        < (SELECT AVG_%s FROM %s_%s WHERE %s = 4)
    THEN 'Yes'
    ELSE 'No'
    END)),''' % (item, self.table_name, main_column, main_column,
                 item, self.table_name, main_column, main_column,
                 item, self.table_name, main_column, main_column,
                 item, self.table_name, main_column, main_column,
                 item, self.table_name, main_column, main_column,
                 item, self.table_name, main_column, main_column)

        query_part = query_part[:-1]
        case_query_part = case_query_part[:-1]  # Cut the coma at the END.

        # Combine the columns query and the CASE query in the main query.
        yn_insert = '''
                INSERT INTO %s_cs(%s_c, %s)
                VALUES(\'%s\', %s
                );
                    ''' % (self.table_name, self.database, query_part,
                           main_column, case_query_part)

        conn = mysql.connector.connect(host='localhost', user='root',
                                       password='dance')
        cursor = conn.cursor()
        cursor.execute(yn_insert)
        conn.commit()
        conn.close()


# Functions call.
def call_functions():
    # Change the database and table names below with the ones you chose.
    meas_to_class = m_to_class('your_database.your_table')
    meas_to_class.get_col_names()
    meas_to_class.copy_main_table()
    meas_to_class.table_columns.remove('id')

    for item in meas_to_class.table_columns:
        meas_to_class.class_values(item)

    # Append the 'id' back to be used in INNER JOIN.
    meas_to_class.table_columns.append('id')

    number = 0
    for item in meas_to_class.table_columns:
        number = number + 1
        meas_to_class.range_avg(item, meas_to_class.table_columns)

    meas_to_class.yes_no_similarity()

    # Remove the 'id' so we don't calculate it as a normal measure column.
    meas_to_class.table_columns.remove('id')

    # Compare every protein concentration to every other protein concentration.
    for item in meas_to_class.table_columns:
        meas_to_class.yes_no_insert(item)


if __name__ == '__main__':
    call_functions()
