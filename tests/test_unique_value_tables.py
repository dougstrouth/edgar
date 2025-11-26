import duckdb
import os
import tempfile
from data_processing.unique_value_tables import create_ai_unique_value_table


def test_unique_value_population_and_normalization():
    # use in-memory duckdb for tests
    con = None
    try:
        con = duckdb.connect(database=':memory:', read_only=False)
        # create sample xbrl_facts
        con.execute('CREATE TABLE xbrl_facts (cik VARCHAR, fy VARCHAR, unit VARCHAR, value_numeric DOUBLE, round_lot INTEGER);')
        con.execute("INSERT INTO xbrl_facts VALUES ('0000123456','2024','USD',1000.0,100), ('0000000001','2023','USD',200.0,50), ('0000999999','2022','EUR',300.0,100);")
        # create sample tickers
        con.execute('CREATE TABLE tickers (cik VARCHAR, ticker VARCHAR, active VARCHAR, round_lot INTEGER);')
        con.execute("INSERT INTO tickers VALUES ('0000123456','ABC','True',100), ('0000000001','XYZ','False',100);")

        # run the unique-value table creation
        create_ai_unique_value_table(con, source_tables=['xbrl_facts', 'tickers'])

        # fetch cik normalized values
        rows = con.execute("SELECT raw_value, normalized_value FROM ai_unique_values WHERE column_name='cik' ORDER BY sample_count DESC;").fetchall()
        assert any(r[1] == '123456' for r in rows), f"Expected normalized cik '123456' in {rows}"
        assert any(r[1] == '1' or r[1] == '0000000001'.lstrip('0') for r in rows)

        # unit USD should be detected as monetary
        usd_row = con.execute("SELECT is_monetary FROM ai_unique_values WHERE column_name='unit' AND raw_value='USD' LIMIT 1;").fetchone()
        assert usd_row is not None and usd_row[0] == True

        # fy should NOT be flagged as monetary
        fy_row = con.execute("SELECT is_monetary FROM ai_unique_values WHERE column_name='fy' AND raw_value='2024' LIMIT 1;").fetchone()
        assert fy_row is not None and fy_row[0] == False

        # round_lot should NOT be flagged as monetary (excluded by column name)
        rl_row = con.execute("SELECT is_monetary FROM ai_unique_values WHERE column_name='round_lot' AND raw_value='100' LIMIT 1;").fetchone()
        assert rl_row is not None and rl_row[0] == False

    finally:
        if con:
            con.close()
