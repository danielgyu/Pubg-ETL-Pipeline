class LoadQueries:
    warehouse_insert = (
        """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS parquet
        """
    )

sql = ("""
       COPY locations
       FROM 's3://capstone-pubg/staging/location/20201225'
       ACCESS_KEY_ID 'AKIAYXIPHEG526FIGU52'
       SECRET_ACCESS_KEY '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'
       FORMAT AS PARQUET
       """)

