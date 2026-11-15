def replace_by_keys(df, table_name, diff_table_name, keys, conn, cursor, engine,
                    diff_columns=None):
    """以 keys 為鍵做 delete-insert：先刪掉 table 內與 df 同鍵的舊資料，再整批 append df。

    步驟:
      1. TRUNCATE diff 表後，把 df 的 diff_columns(預設=keys) 寫進去
      2. 刪除 table_name 中在 diff 表出現過的 keys
      3. df 整批 append 到 table_name

    回傳 (delete_count, insert_count)。
    """
    cursor.execute(f'TRUNCATE TABLE {diff_table_name}')
    conn.commit()

    df.loc[:, list(diff_columns or keys)].to_sql(
        diff_table_name, engine, index=False, if_exists='append',
    )

    key_conditions = ' and '.join(f'tb.{key}=tb2.{key}' for key in keys)
    delete_count = cursor.execute(
        f"""
        delete tb from {table_name} as tb
        where exists (
            select 1
            from {diff_table_name} tb2
            where {key_conditions}
        )
        """
    )
    conn.commit()

    df.to_sql(table_name, engine, index=False, if_exists='append')
    return delete_count, len(df)
