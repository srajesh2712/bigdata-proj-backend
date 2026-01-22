import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

def parquet_basics():
    data = {
        'match_id':range(1,10001),
        'stadium':['MetLife']*5000+['Azteca']*5000,
        'goals':[i%5 for i in range(10000)],
    }
    df = pd.DataFrame(data)
    df_sorted = df.sort_values('goals').reset_index(drop=True)
    table = pa.Table.from_pandas(df)
    pq.write_table(table, 'fifa_test.parquet', row_group_size=5000)
    print("File 'fifa_test.parquet' created successfully!")
    parquet_file = pq.ParquetFile('fifa_test.parquet')

    print(f"\n--- Internal Structure of {parquet_file.metadata.num_rows} rows ---")
    print(f"Number of Row Groups: {parquet_file.num_row_groups}")
    print(f"Number of Columns: {parquet_file.metadata.num_columns}")




    # 5. Interrogate the 'Librarian' (Metadata) for the first Row Group
    first_group = parquet_file.metadata.row_group(0)
    print(f"\nRow Group 0 Statistics for 'goals' column:")
    print(f"  Total Rows: {first_group.num_rows}")
    print(f"  Min Goals: {first_group.column(2).statistics.min}")
    print(f"  Max Goals: {first_group.column(2).statistics.max}")
    print(f"  Min Goals: {first_group.column(2).statistics}")
    print(f"  Max Goals: {first_group.column(1).statistics}")

def parquet_joins():
        df1 = pd.DataFrame({'stadium': ['MetLife']*5, 'goals': [1, 2, 1, 0, 1]})
        df2 = pd.DataFrame({'stadium': ['Azteca']*5,  'goals': [12, 0, 15, 1, 2]}) # Matches here!
        df_combined = pd.concat([df1, df2])
        df_sorted = df_combined.sort_values('goals').reset_index(drop=True)
        table = pa.Table.from_pandas(df_sorted)
        # 2. Write with 2 distinct Row Groups
        pq.write_table(table, 'fifa_pushdown.parquet', row_group_size=5)

        parquet_file = pq.ParquetFile('fifa_pushdown.parquet')
        print(parquet_file.metadata)

        filtered_table = pq.read_table('fifa_pushdown.parquet',
                                       columns=['stadium'],
                                       filters=[('goals','=',2)])
        print("Results found in the 'Gold' stadium list:")
        print(filtered_table.to_pandas())

def parquet_partition():
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq

    df = pd.DataFrame({
        'stadium': ['MetLife', 'MetLife', 'Azteca', 'Azteca', 'MetLife'],
        'goals': [1, 1, 1, 2, 2]
    })

    table = pa.Table.from_pandas(df)

    # Instead of write_table, use write_to_dataset
    pq.write_to_dataset(table, root_path='fifa_data', partition_cols=['goals'])

#parquet_basics()
#parquet_joins()
parquet_partition()