# ptransform.py
import polars as pl

def eliminar_columnas(df: pl.DataFrame) -> pl.DataFrame:
    columns_to_remove = ['address','description','price','hours','MISC','state','relative_results']
    return df.drop(columns_to_remove)

def explode_columna(df: pl.DataFrame, columna_a_explodar: str) -> pl.DataFrame:
    return df.explode(columna_a_explodar)

def eliminar_duplicados(df: pl.DataFrame, columna_a_verificar: str) -> pl.DataFrame:
    return df.unique(subset=[columna_a_verificar],keep='first')

def filtrar_por_categoria(df: pl.DataFrame, columna_a_filtrar: str, keyword: str) -> pl.DataFrame:
    return df.filter(df[columna_a_filtrar].str.contains(f'(?i){keyword}'))
