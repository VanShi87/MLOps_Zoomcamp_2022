from datetime import datetime
import os
import pandas as pd


def dt(hour: int, minute: int, second: int=0) -> datetime:
    return datetime(2021, 1, 1, hour, minute, second)


if __name__ == '__main__':
    data = [
        (None, None, dt(1, 2), dt(1, 10)),
        (1, 1, dt(1, 2), dt(1, 10)),
        (1, 1, dt(1, 2, 0), dt(1, 2, 50)),
        (1, 1, dt(1, 2, 0), dt(2, 2, 1)),
    ]

    columns = ['PUlocationID', 'DOlocationID', 'pickup_datetime', 'dropOff_datetime']
    df_input = pd.DataFrame(data, columns=columns)

    options = {
        'client_kwargs': {
            'endpoint_url': 'http://localhost:4566'
        }
    }

    input_file = "s3://nyc-duration/in/2021-01.parquet"

    df_input.to_parquet(
        input_file,
        engine='pyarrow',
        compression=None,
        index=False,
        storage_options=options
    )
    os.system('python3 batch.py 2021 1')

	
