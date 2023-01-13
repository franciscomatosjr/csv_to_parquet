from pyarrow import csv, parquet, feather
from datetime import datetime
import pandas as pd
import dask.dataframe as dd
import os


def file_to_data_frame_to_parquet(local_file: str, parquet_file: str, encoding: str = 'utf8', compression: str ='snappy') -> None:
    try:
        parse_options = csv.ParseOptions(delimiter=";")
        data_arrow = csv.read_csv(local_file, parse_options=parse_options, read_options=csv.ReadOptions(autogenerate_column_names=True, encoding=encoding))

        if compression == 'GZIP':
            extensao_arquivo = '.parquet.gzip'
        elif compression == 'BROTLI':
            extensao_arquivo = '.parquet.brotli'
        else:
            extensao_arquivo = '.parquet'

        parquet.write_table(data_arrow, parquet_file + extensao_arquivo, compression=compression)

        return print("Arquivo salvo com sucesso")

    except Exception as error:
        raise Exception(f"Ocorreu um erro ao tentar converter o arquivo para parquet. MSG: {error}")


def convert_parquet_to_dataframe(local_file: str):
    t1 = datetime.now()

    df = pd.read_parquet(local_file, engine='pyarrow')
    t2 = datetime.now()

    took = t2 - t1
    print(f"it took {took} seconds to convert parquet to dataframe.")

    return df


def read_csv_to_dataframe(local_file, compression: str = 'lz4', encoding: str = 'utf8', header: None = 'infer', decimal: str = "."):
    df_raw = pd.read_csv(local_file, sep=";", dtype=str, decimal=decimal, encoding=encoding, header=header)

    return df_raw

def convert_csv_to_feather(local_file, compression: str = 'lz4', encoding: str='utf8', header : None = 'infer'):

    """

    :param local_file:
    :param compression: lz4 or zstd
    :param encoding:
    :return:
    """

    diretorio = os.path.realpath(local_file)

    # df_raw = pd.read_csv(local_file, sep=";", dtype=str, decimal=',', encoding=encoding, header=None, nrows=10)
    df_raw = pd.read_csv(local_file, sep=";", dtype=str, decimal=',', encoding=encoding, header=None)

    dict_columns = {}

    for column_name in df_raw.columns:
        dict_columns[column_name] = str(column_name)

    print(dict_columns)

    df_raw.rename(columns=dict_columns, inplace=True)

    file_name = os.path.basename(local_file).split('/')[-1]

    # df_feather = df_raw.to_feather(diretorio + file_name[:-5] + f'.{compression}')
    df_feather = df_raw.to_feather(f'Estabelecimento.{compression}', compression=compression)

    return True

def read_feather_to_df(local_file, encoding: str='utf8'):
    df_feather = pd.read_feather(local_file)
    return df_feather



if __name__ == "__main__":


    local_csv_file = "Estabelecimentos0.csv"

    t1 = datetime.now()
    df_raw = read_csv_to_dataframe(local_csv_file, header=None, encoding='latin1', decimal=',')
    t2 = datetime.now()
    took = t2 - t1
    print(f"it took {took} seconds to convert csv to DataFrame.")


    # Convert cst to parquet

    t1 = datetime.now()
    compression = 'BROTLI'
    file_to_data_frame_to_parquet(local_csv_file, local_csv_file, encoding='latin1', compression=compression)
    t2 = datetime.now()
    took = t2 - t1
    print(f"it took {took} seconds to write csv to parquet.")

    df_parquet = convert_parquet_to_dataframe("Estabelecimentos.parquet.parquet.brotli")


    # Convert CSV to Feather
    t1 = datetime.now()
    convert_csv_to_feather(local_csv_file, encoding='latin1', header=None, compression='zstd')

    t2 = datetime.now()
    took = t2 - t1
    print(f"it took {took} seconds to convert csv to feather.")

    # Read feather to Dataframe
    t1 = datetime.now()
    local_csv_file = "Estabelecimento.zstd"
    df = read_feather_to_df(local_csv_file)

    t2 = datetime.now()
    took = t2 - t1
    print(f"it took {took} seconds to read csv to DataFrame.")
