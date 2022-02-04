# # Add Cairn Repo to PATH
# import os
# cwd = os.getcwd()
# dir_path = cwd.split("inpi_app")[0]
# import sys
# sys.path.append(dir_path)


import pandas as pd
import zipfile
import xmltodict
from pathlib import Path
import tempfile
import shutil
import os
from pandarallel import pandarallel
from os import walk
import configparser


pd.options.display.max_columns = 999

def _year_comptes(date_cloture_exercice):
    mois = int(date_cloture_exercice[-4:-2])
    if mois > 6:
        return int(date_cloture_exercice[:4])
    else:
        return int(date_cloture_exercice[:4]) - 1


def compute_cairn_metrics(df):

    def _df(col='AA'):
        try:
            return df[col]
        except:
            print(f"{col} not present")
            return 0

    liasses_columns = [col for col in df.columns if len(col) == 2]
    df[liasses_columns] = df[liasses_columns].fillna(0)

    df['C_IS_Sales'] = +_df('FL')
    df['IS_Produits_dExplotation'] = +_df('FR')
    df['IS_Charges_dExplotation'] = +_df('GF')
    df['IS_Resultat_dExplotation'] = +_df('FR') - _df('GF')
    df['IS_Increase_Finished_Inventory'] = +_df('FM') + _df('FN')
    df['IS_Costs_Purchases'] = +_df('FS') + _df('FT') + _df('FU') + _df('FV')
    df['IS_Costs_External_Services'] = +_df('FW')
    df['IS_Costs_Personnal'] = +_df('FY') + _df('FZ') + _df('HJ')
    df['IS_Costs_Taxes'] = +_df('FX') - _df('FO')
    df['IS_Costs_Other'] = +_df('GE') - _df('FQ')
    df['IS_Increase_Provisions'] = -_df('FP') + _df('GB') + _df('GC') + _df('GD')
    df['IS_Costs_D_A'] = +_df('GA') + _df('GB') + _df('GC') + _df('GD')
    df['C_IS_EBIT'] = +_df('IS_Resultat_dExplotation') - _df('HJ') - _df('FM') - _df('FN')
    df['C_IS_EBITDA'] = +_df('C_IS_EBIT') + _df('IS_Costs_D_A')
    df['C_IS_Cash_Costs'] = +_df('C_IS_Sales') - _df('C_IS_EBITDA')

    df = compute_agg_BS_sheet(df)

    df['C_IS_EBITDA_Margin'] = df['C_IS_EBITDA'] / df['C_IS_Sales']
    df['C_IS_EBIT_Margin'] = df['C_IS_EBIT'] / df['C_IS_Sales']
    df['C_IS_NOPAT'] = df['C_IS_EBIT'] * (1-28/100)
    df['C_BS_Adj_ROCE'] = df['C_IS_NOPAT'] / df['C_BS_Capital_Employed_Adj']
    df['C_IS_NOPAT_Margin'] = df['C_IS_NOPAT'] / df['C_IS_Sales']
    df['C_BS_Sales_to_CE'] = df['C_IS_Sales'] / df['C_BS_Capital_Employed_Adj']
    df['C_BS_Leverage'] = df['C_IS_EBITDA'] / df['C_BS_Net_Debt']
    df['C_IS_Personnel_to_Sales'] = df['IS_Costs_Personnal'] / df['C_IS_Sales']

    return df

def extract_comptes_sociaux_from_file(file):
    mapping = pd.read_excel(os.path.join(os.environ["CAIRN_DIR"], "utils", "inpi_helpers", "liasses_mapping.xlsx"))
    mapping = mapping[["key", "col_number", "starting_from"]]
    try:
        bilan_def = pd.Series(file["bilans"]["bilan"]["identite"])



        pages = file["bilans"]["bilan"]["detail"]["page"]

        liasses = [l for page in pages for l in page["liasse"] if isinstance(l, dict)]
        liasses = pd.DataFrame(liasses)
        liasses = liasses.set_index("@code")
        liasses = liasses.astype(float).fillna(0)
        res = pd.merge(mapping, liasses, left_on="starting_from", right_index=True, how="left")

        def get_value(x):
            col_number = x["col_number"]
            return x[f"@m{col_number}"]

        res["value"] = res.apply(get_value, axis=1)
        res = res[["key", "value"]].set_index("key")["value"]
        res = res.astype(float)

        # return pd.concat([bilan_def, res], ignore_index=False)
        final_dict = pd.concat([bilan_def, res], ignore_index=False)
        return final_dict
    except:
        return pd.Series(dtype="object")


def compute_agg_BS_sheet(df):
    # AGG_BS
    def _df(col):
        try:
            return df[col]
        except:
            print(f"{col} not present")
            return 0

    df['BS_Actif'] = +_df('CO') - _df('1A')
    df['BS_Passif'] = +_df('EE')
    df['Check_Actf_Passif'] = +_df('BS_Actif') - _df('BS_Passif')
    df['BS_Accrued_Expenses'] = +_df('CH') - _df('CI')
    df['BS_Accrued_Revenue'] = +_df('EB')
    df['BS_Assets_Intangible'] = +_df('AJ') - _df('AK') + _df('AL') - _df('AM') + _df('AF') - _df('AG') + _df('AB') - _df('AC') + _df('CX') - _df('CQ')
    df['BS_Assets_PPE'] = +_df('AT') - _df('AU') + _df('AX') - _df('AY') + _df('AP') - _df('AQ') + _df('AV') - _df('AW') + _df('AR') - _df('AS') + _df('AN') - _df('AO')
    df['BS_Cash'] = +_df('BH') - _df('BI') + _df('CF') - _df('CG') + _df('BF') - _df('BG') + _df('CD') - _df('CE') + _df('AA')
    df['BS_Cash_Other'] = +_df('CW') - +_df('CM')
    df['BS_Debt'] = +_df('EA') + _df('DT') + _df('DU') + _df('DV') + _df('DS')
    df['BS_Equity'] = +_df('DO') + _df('DL')
    df['BS_Actif_Ecart'] = +_df('CN')
    df['BS_Passif_Ecart'] = +_df('ED')
    df['BS_Goodwill'] = +_df('AH') - _df('AI')
    df['BS_Minority'] = +_df('CU') - _df('CV') + _df('BD') - _df('BE') + _df('BB') - _df('BC') + _df('CS') - _df('CT')
    df['BS_Payables'] = +_df('DW') +_df('DX')
    df['BS_Payables_Other'] = +_df('DY') +_df('DZ')
    df['BS_Provisions'] = +_df('DR')
    df['BS_Receivables'] = +_df('BZ') - _df('CA') + _df('BV') - _df('BW') + _df('CB') - _df('CC') + _df('BX') - _df('BY')
    df['BS_Stocks'] = +_df('BN') - _df('BO') + _df('BP') - _df('BQ') + _df('BT') - _df('BU') + _df('BL') - _df('BM') + _df('BR') + _df('BS')
    df['BS_Assets_Total'] = +_df('BS_Accrued_Expenses') + _df('BS_Assets_Intangible') + _df('BS_Assets_PPE') + _df('BS_Cash') + _df('BS_Cash_Other') + _df('BS_Actif_Ecart') + _df('BS_Goodwill') + _df('BS_Minority') + _df('BS_Receivables') + _df('BS_Stocks')
    df['Check_Total_Assets'] = +_df('BS_Assets_Total') - _df('BS_Actif')
    df['BS_L_E_Total'] = +_df('BS_Accrued_Revenue') + _df('BS_Debt') + _df('BS_Equity') + _df('BS_Passif_Ecart') + _df('BS_Payables') + _df('BS_Payables_Other') + _df('BS_Provisions')
    df['Check_Total_L_E'] = +_df('BS_L_E_Total') - _df('BS_Passif')
    df['C_BS_Net_Fixed_Assets'] = +_df('BS_Assets_Intangible') + _df('BS_Assets_PPE') + _df('BS_Minority')
    df['C_BS_Working_Capital'] = +_df('BS_Receivables') + _df('BS_Stocks') + _df('BS_Accrued_Expenses') - _df('BS_Accrued_Revenue') - _df('BS_Payables') - _df('BS_Payables_Other')
    df['C_BS_Net_Debt'] = +_df('BS_Debt') - _df('BS_Cash') - _df('BS_Cash_Other')
    df['C_BS_Equity_Adj'] = +_df('BS_Equity') - _df('BS_Goodwill') + _df('BS_Passif_Ecart') - _df('BS_Actif_Ecart') + _df('BS_Provisions')
    df['BS_Capital_Employed'] = +_df('C_BS_Net_Fixed_Assets') + _df('C_BS_Working_Capital')
    df['BS_Minimum_Cash'] = round(_df('C_IS_Cash_Costs') * 4 / 52 ,0)
    df['C_BS_Capital_Employed_Adj'] = +_df('BS_Capital_Employed') + _df('BS_Minimum_Cash')
    df['BS_Working_Capital_Op'] = +_df('BS_Receivables') + _df('BS_Stocks') - _df('BS_Payables')
    df['C_BS_PPE_Gross'] = +_df('AT') + _df('AX') + _df('AP') + _df('AV') + _df('AR') + _df('AN')
    return df

def _extract_zip_file(zip_list, extraction_path):
    extracted_absolute_path = []
    extracted_filenames = []
    for i, zip_file in enumerate(zip_list):
        with zipfile.ZipFile(zip_file, 'r') as zipFile:
            zipFile.extractall(extraction_path)
            extracted = zipFile.namelist()
            extracted_absolute_path.extend([os.path.join(extraction_path, file) for file in extracted])
            extracted_filenames.extend(extracted)
    return extracted_absolute_path, extracted_filenames


def extract_inpi(zip_file, parquet_path):
    _sufix = str(Path(zip_file).parent).split("Bilans_Donnees_Saisies/")[1]
    year = _sufix.split("/")[1]
    month = _sufix.split("/")[2]
    zip_extracted_1 = []
    zip_extraction_path = tempfile.mkdtemp()
    # print(zip_extraction_path)
    #  Path(zip_extraction_path).mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_file, 'r') as zipFile:
        zipFile.extractall(zip_extraction_path)
        extracted = zipFile.namelist()
        zip_extracted_1.extend([os.path.join(zip_extraction_path, file) for file in extracted])

        zip_extracted_1_zips = [file for file in zip_extracted_1 if ".zip" in file]
        extracted_absolute_path, extracted_filenames = _extract_zip_file(zip_extracted_1_zips, zip_extraction_path)

        res = []
        for xml_path, filename in zip(extracted_absolute_path, extracted_filenames):
            if "453292088" in xml_path:
                # debug_path = "/Users/bletournel/Documents/perso/cairn/data/inpi_ftp_parquet_debug/"
                # Path(debug_path).mkdir(parents=True, exist_ok=True)
                # copyfile(xml_path, os.path.join(debug_path, filename))
                print("Found file")
                print(f"Zip file is : {zipFile}")
            if ".xml" in xml_path:
                with open(xml_path, 'r', encoding="utf-8") as myfile:
                    obj = xmltodict.parse(myfile.read(), encoding="utf-8")
                try:
                    pages = obj["bilans"]["bilan"]["detail"]["page"]
                    pages_number = [page["@numero"] for page in pages]
                    # 01 = Actif
                    # 02 = Passif
                    # 03 = CR
                    # 04 = Immo
                    # 05 = Amortissements
                    # 06 = Provisions
                    # 07 = Creances et dettes
                    # 08 = Affectations resultats
                    if "03" not in pages_number and "02" in pages_number:
                        # exemple 840225312: Nous n'avons pas le CR mais nous pouvons extraire le resultat net du Passif
                        pass
                    if "03" in pages_number or "02" in pages_number:  # compte de résultat ou passif
                        comptes_sociaux = extract_comptes_sociaux_from_file(obj)
                        if comptes_sociaux["DI"] and comptes_sociaux["DI"] != 0:
                            res.append(comptes_sociaux)

                except Exception as e:
                    # print(e)
                    pass
    if res:
        res = pd.concat(res, axis=1).T
        float_cols = [col for col in res.columns if len(col) == 2]
        res[float_cols] = res[float_cols].astype(float)
        res["year"] = year
        res["month"] = month
        Path(parquet_path).mkdir(parents=True, exist_ok=True)

        # table = pa.Table.from_pandas(res)
        # pq.write_to_dataset(table, root_path=parquet_path, partition_cols=["year", "month"])

        res.to_parquet(parquet_path, partition_cols=["year", "month"])

    #TODO: copy file to another folder when done
    # Path(debug_path).mkdir(parents=True, exist_ok=True)
    # copyfile(xml_path, os.path.join(debug_path, filename))

    # Delete tmp folder
    path = Path(zip_extraction_path)
    shutil.rmtree(path)
    return "done"

import click

@click.command()
@click.option('--mode', default="test_local", help='Mode in config')
def run_unzip_all(mode):
    config = configparser.ConfigParser()
    config.read('conf.ini')
    conf = config[mode]
    if "input_dir" in conf.keys():
        input_dir = conf["input_dir"]
        f = []
        for (dirpath, dirnames, filenames) in walk(input_dir):
            #  print(dirpath, dirnames, filenames)
            f.extend([os.path.join(dirpath, filename) for filename in filenames])
        f = [file for file in f if ".zip" in file]
    elif "input_file" in conf.keys():
        f = [conf["input_file"]]
    parquet_path = conf["parquet_path"]

    print(f"Saving to {parquet_path}")
    df = pd.DataFrame(f, columns=["zips"])
    if mode == "test_local" or mode == "test_remote":
        df["zips"].apply(extract_inpi, **{"parquet_path" : parquet_path})
    # elif mode == "debug":
    #     pandarallel.initialize(nb_workers=10, progress_bar=False)
    #     df["status"] = df["zips"].parallel_apply(extract_inpi, **{"parquet_path": parquet_path})
    else:
        pandarallel.initialize(progress_bar=False) # progress bar has issue
        df = df.sample(frac=1)
        df["status"] = df["zips"].parallel_apply(extract_inpi, **{"parquet_path": parquet_path})


    # reading results
    df = pd.read_parquet(parquet_path)

    #  removing true duplicates
    df = df[[col for col in df.columns if col not in ["year", "month"]]].drop_duplicates()

    # Removing other duplicates
    code_motif_order = ["00", "01", "1A", "06"]
    df["code_motif"] = pd.Categorical(df["code_motif"], categories=code_motif_order)
    code_type_bilan_order = ["C", "K", "S", "B", "A"]
    df["code_type_bilan"] = pd.Categorical(df["code_type_bilan"], categories=code_type_bilan_order)
    df = df.sort_values(by=["siren", "date_cloture_exercice", "code_type_bilan", "code_motif"])
    df = df.drop_duplicates(["siren", "date_cloture_exercice"])

    liasses_cols = [col for col in df.columns if len(col) == 2]
    df[liasses_cols] = round(df[liasses_cols] / 1000, 0)

    df = compute_cairn_metrics(df)
    df["year"] = df["date_cloture_exercice"].astype(str).apply(_year_comptes)
    df.rename(columns={"siren": "SIREN"}, inplace=True)
    df["year"] = df["date_cloture_exercice"].str[:4]
    df["from"] = "inpi"
    df = df.sort_values(["SIREN", "year", "C_IS_Sales"], ascending=(True, True, False)).drop_duplicates(["SIREN", "year", ])

    if conf["merge_with_dodo"] == "True":
        # Merge with dodo
        dodo = pd.read_csv(conf["dodo_file"])
        print("Dodo loaded")

        liasses_col = []
        for col in dodo.columns:
            _ = col.split(" ")[0]
            if len(col.split(" ")[0]) == 2 and "1" not in _ and _ != "PC":
                liasses_col.append(col)
                dodo.rename(columns={col: _}, inplace=True)

        dodo = compute_cairn_metrics(dodo)
        dodo = dodo.rename(columns={"Années de l'exercice": "year"})
        dodo["from"] = "dodo"
        dodo = dodo.drop_duplicates()
        dodo["SIREN"] = dodo["SIREN"].astype(str).apply(lambda x: str(x).zfill(9))
        dodo["year"] = dodo["year"].astype(str)

        dodo = dodo.sort_values(["SIREN", "year", "C_IS_Sales"], ascending=(True, True, False)).drop_duplicates(["SIREN", "year", ])
        # dodo = dodo.astype(df.dtypes.to_dict())

        columns_in_common = df.columns & dodo.columns

        res = pd.concat([
            df[columns_in_common],
            dodo[columns_in_common]
        ])
    else:
        res = df

    deduplications = res[res.duplicated(["SIREN", "year"], keep=False)].sort_values(["SIREN", "year", "from"],
                                                                        ascending=(True, True, True))
    deduplications.head()

    # if duplicates Keeping Dodo version
    res = res.sort_values(["SIREN", "year", "from"], ascending=(True, True, True)).drop_duplicates(["SIREN", "year"])

    res["SIREN"] = res["SIREN"].astype(str).str.zfill(9)
   # renaming columns for SQL server
    liasses_cols = [col for col in res.columns if len(col) == 2]
    for col in liasses_cols:
        res.rename(columns={col: "_" + col}, inplace=True)
    res.rename(columns={"from": "source"}, inplace=True)

    res.to_parquet(conf["save_path"], index=False)
    #
    # # #TODO save locally then upload to s3
    # # import pandas as pd
    # # import pyarrow as pa
    # # import pyarrow.parquet as pq
    #
    # res.to_parquet(path=conf["dodo_inpi_file"], engine='auto', compression='snappy', partition_cols=["SIREN"])

if __name__ == "__main__":
    run_unzip_all()
    """
    On VM run  
    """
