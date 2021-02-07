# Numeric Attribute Correlation

import mysql.connector
import unittest
import numeric_attribute_correlation as n_a_c


class correlation_test(unittest.TestCase):

    table_path = 'test_correlation.protein_measures'
    correlation = n_a_c.m_to_class(table_path)

    def test_1_main_t(self):
        # Check if the main table has the right data.

        self.correlation.get_col_names()
        self.assertEqual(self.correlation.table_columns,
                         ['DYRK1A_N', 'id', 'ITSN1_N', 'pAKT_N',
                          'pCAMKII_N', 'pCREB_N'],
                         self.correlation.table_columns)

    def test_2_copies(self):
        # Check if the script makes 2 proper copies of the main table.

        self.correlation.copy_main_table()

        query = """
            SELECT * FROM %s_copy_2 LIMIT 1;
                """ % self.table_path

        conn = mysql.connector.connect(host='localhost', user='root',
                                       password='dance')
        cursor = conn.cursor()
        cursor.execute(query)
        self.result = cursor.fetchall()
        conn.commit()

        self.assertEqual(self.result[0],
                         (1, 0.503643884, 0.747193224, 2.373744337,
                          0.218830018, 0.232223754),
                         '(1, 0.503643884, 0.747193224, 2.373744337,\
                          0.218830018, 0.232223754)')

    def test_3_four_ranges(self):
        # Check if the second copy table(protein_measures.copy_2) changes all
        # it\'s values to 1:4 numbers properly.
        self.correlation.get_col_names()
        self.correlation.table_columns.remove('id')
        for item in self.correlation.table_columns:
            self.correlation.class_values(item)

        query = """
            SELECT * FROM %s_copy_2 LIMIT 1;
                """ % self.table_path
        conn = mysql.connector.connect(host='localhost', user='root',
                                       password='dance')

        cursor = conn.cursor()
        cursor.execute(query)
        self.result = cursor.fetchall()
        conn.commit()

        self.assertEqual(self.result[0], (1, 2.0, 2.0, 1.0, 2.0, 3.0),
                         'Tupple not the same.')

    def test_4_auxiliary_tables(self):
        # 2 tests fitted in here. 2 of the intermediate tables are checked.

        self.correlation.get_col_names()

        for item in self.correlation.table_columns:
            self.correlation.range_avg(item, self.correlation.table_columns)

        query_2 = """
                SELECT AVG_ITSN1_N, AVG_pCREB_N
                FROM %s_dyrk1a_n LIMIT 1;;
                  """ % self.table_path

        conn = mysql.connector.connect(host='localhost', user='root',
                                       password='dance')

        cursor = conn.cursor()
        cursor.execute(query_2)
        self.result = cursor.fetchall()
        conn.commit()

        self.assertEqual(self.result[0], (0.48102500550922495,
                         0.2037136183136532), 'Tuple not the same!')

        query_3 = """
                SELECT AVG_ITSN1_N, AVG_pCREB_N
                FROM %s_pakt_n LIMIT 1;
                  """ % self.table_path

        conn = mysql.connector.connect(host='localhost', user='root',
                                       password='dance')

        cursor = conn.cursor()
        cursor.execute(query_3)
        self.result = cursor.fetchall()
        conn.commit()

        self.assertEqual(self.result[0], (0.6668220397343749,
                         0.17247992306250007), 'Tuple not the same!')

    def test_5_yes_no_create(self):
        # Check if the comparrison table is created.

        self.correlation.get_col_names()
        self.correlation.yes_no_similarity()

        self.database_name = self.table_path.split('.')[0]

        query = """
                SHOW TABLES FROM %s;
                """ % self.database_name

        conn = mysql.connector.connect(host='localhost', user='root',
                                       password='dance')

        cursor = conn.cursor()
        cursor.execute(query)
        self.result = cursor.fetchall()
        conn.commit()

        self.assertTrue(('protein_measures_cs',) in self.result,
                        'Tuple not the same!')

    def test_6_yes_no_insert(self):
        # Check if the protein correlation comparison data is correct.

        self.correlation.get_col_names()
        self.correlation.table_columns.remove('id')

        for item in self.correlation.table_columns:
            self.correlation.yes_no_insert(item)

        query = """
                SELECT * FROM test_correlation.protein_measures_cs LIMIT 1;
                """

        conn = mysql.connector.connect(host='localhost', user='root',
                                       password='dance')

        cursor = conn.cursor()
        cursor.execute(query)
        self.result = cursor.fetchall()
        conn.commit()

        # Check if the first line of the data is correct.
        self.assertEqual(self.result[0],
                         (1, 'DYRK1A_N', None, 'Yes', 'No', 'No', 'No'),
                         'Tuple not the same.')

    def test_7_nullify(self):

        # Delete a row to make a table unusable.
        query = """
            DELETE FROM test_correlation.protein_measures_pakt_n
            WHERE pakt_n = 3;
                """

        conn = mysql.connector.connect(host='localhost', user='root',
                                       password='dance')

        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()

        # Check for unusable tables.
        self.correlation.column_nullify()

        query_2 = """
                SELECT * FROM test_correlation.protein_measures_cs
                WHERE test_correlation_c = 'DYRK1A_N'
                  """

        conn = mysql.connector.connect(host='localhost', user='root',
                                       password='dance')

        cursor = conn.cursor()
        cursor.execute(query_2)
        self.result = cursor.fetchall()
        conn.commit()

        # Check if the first row of the comparrison table has changed
        # properly after the deliberate data damage.
        self.assertEqual((self.result[0]),
                         (1, 'DYRK1A_N', None, 'Yes', 'No', None, 'No'),
                         'Tuples not the same')

        # Deletre all the tables used in the test.
        query_3 = """
                DROP TABLES
                test_correlation.protein_measures_copy,
                test_correlation.protein_measures_copy_2,
                test_correlation.protein_measures_cs,
                test_correlation.protein_measures_dyrk1a_n,
                test_correlation.protein_measures_id,
                test_correlation.protein_measures_itsn1_n,
                test_correlation.protein_measures_pakt_n,
                test_correlation.protein_measures_pcamkii_n,
                test_correlation.protein_measures_pcreb_n;
                    """

        conn = mysql.connector.connect(host='localhost', user='root',
                                       password='dance')

        cursor = conn.cursor()
        cursor.execute(query_3)
        conn.commit()


# Run the tests.
if __name__ == '__main__':
    unittest.main()
