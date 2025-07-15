from unittest import TestCase, main
from unittest.mock import patch, MagicMock, call

import pandas as pd

from src import db_tools


class TestDBConn(TestCase):

    def setUp(self):
        setup_path = "Some/path/to/db.db"
        self.dbconn = db_tools.DBConn(setup_path)
        self.dbconn.conn = MagicMock()
        self.dbconn.cursor = MagicMock()

    # @patch("src.db_tools.sqlite3")
    def test_scd_type_two_query(self):
        test_data = {
            'start_date': ['2023-01-01', '2023-05-15', '2024-03-20'],
            'stop_date': ['2023-02-01', '2023-06-15', '2024-04-20'],
            'conflict': ['Conflict A', 'Conflict B', 'Conflict C'],
            'party': ['Party X', 'Party Y', 'Party Z'],
            'category_name': ['Category 1', 'Category 2', 'Category 3'],
            'type_name': ['Type Alpha', 'Type Beta', 'Type Gamma'],
            'loss_id': [101, 102, 103],
            'loss_type': ['Destroyed', 'Damaged', 'Abandoned'],
            'proof_id': [111, 222, 333],
        }
        fake_pandas_df = pd.DataFrame(test_data)
        print(fake_pandas_df.to_string())

        sql = self.dbconn._scd_type_two_query(fake_pandas_df, "my_table", "2025-07-04")
        print(sql)

        sql2 = self.dbconn._sql_column_filters(fake_pandas_df, ["stop_date"])
        print(sql2)






if __name__ == '__main__':
    main()
