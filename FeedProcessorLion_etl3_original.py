#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from __future__ import print_function
import pandas as pd
from lib_util import pyetl
from lib_util import lib_log
from collections import OrderedDict
import re
from csv import QUOTE_ALL,QUOTE_MINIMAL,QUOTE_NONE,QUOTE_NONNUMERIC
import os
import gc
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP, ROUND_FLOOR, ROUND_CEILING
from batch_lib import *
from lib_util import utilities
from dateutil.parser import parse

def roundoff(n, x):
        x = str(pow(10,-x))
        return Decimal(str(n)).quantize(Decimal(x), rounding =ROUND_HALF_UP)
def remove_lagging_decimal_zero(x):
        return str(x).rstrip('0').rstrip('.') if x is not None else x

logger = lib_log.Logger()

class LoadFile(pyetl.FilePyEtl):
    def __init__(self, base_directory, input_dir, output_dir):
        super(LoadFile, self).__init__()
        self.empty_df_flag = True
        self.base_directory = base_directory
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.files_data = OrderedDict([
        # reading rrpt db realated files. 
           ('index_mappings', {
                "path_or_buf": os.path.join(self.input_dir,
                "index_mappings.txt"),
                "sep": "|",
                "usecols": ["iconum", "ccompany_id", "ccompany_name", "csource"],
                "keep_default_na": False,
                "na_values": ["N/A"],
                "low_memory": False,
                "quoting": QUOTE_ALL,
            }),
           ('fe_broker', {
                "path_or_buf": os.path.join(self.input_dir,
                "fe_broker.txt"),
                "sep": "|",
                "usecols": ["n_broker_id","confidential","restriction_mode",
                    "firmlevelentitlement"],
                "low_memory": False,
                "quoting": QUOTE_ALL,
            }),
           ('rep_contributors', {
                "path_or_buf": os.path.join(self.input_dir,
                "rep_contributors.txt"),
                "sep": "|",
                "usecols": ["c_contrib_id"
                            ,"l_allow_contribution"
                            ,"n_fe_broker_id"
                            ,"b_client_visible"
                            ,"l_ent_visible"
                            ,"internal_visible"
                            ,"cfertype"
                            ,"l_fer_external"
                            ,"l_fer_internal"
                            ,"models_active"
                            ,"models_internal"
                            ,"models_external"
                            ,"models_contributor_type"
                            ,"models_link_back"
                            ,"fe_entitle"
                            ,"entitle_feed"
                            ,"l_watermark"
                            ,"l_linkback"
                            ],
                "dtype": {
                    "c_contrib_id": object},
                "low_memory": False,
                "quoting": QUOTE_ALL,
            }),
        # reading lion db realted files.
           ('filermst', {
                "path_or_buf": os.path.join(self.input_dir,
                "filermst.txt"),
                "sep": "|",
                "usecols": ["iconum", "email"],
                "keep_default_na": False,
                "na_values": [""],
                "low_memory": False,
                "quoting": QUOTE_ALL,
            }),
           ('address', {
                "path_or_buf": os.path.join(self.input_dir,
                "address.txt"),
                "sep": "|",
                "usecols": [
                    "address_id"
                    ,"iconum"
                    ,"loc_city"
                    ,"loc_st_prov"
                    ,"loc_postal_code"
                    ,"loc_str1"
                    ,"loc_str2"
                    ,"loc_str3"
                    ,"iso_country"
                    ,"tele_country"
                    ,"tele_area"
                    ,"tele"
                    ,"fax_country"
                    ,"fax_area"
                    ,"fax"
                    ],
                "keep_default_na": False,
                "na_values": [""],
                "dtype": { 
                    "loc_str1": object
                    ,"loc_str2": object
                    ,"loc_str3": object
                    ,"tele_country": object
                    ,"tele_area": object
                    ,"tele": object
                    ,"fax_country": object
                    ,"fax_area": object
                    ,"fax":object
                },
                "low_memory": False,
                "quoting": QUOTE_ALL,
            }),
           ('inst', {
                "path_or_buf": os.path.join(self.input_dir,
                "inst.txt"),
                "sep": "|",
                "usecols": ["iconum", "address_id", "type_code"],
                "low_memory": False,
                "quoting": QUOTE_ALL,
            }),
        ])
        self.results = OrderedDict([
            ('ac_contributors', {
                "path_or_buf":os.path.join(self.output_dir,
                "ac_contributors.txt"),
                "sep":"|",
                "encoding": 'mbcs',
                "quoting":QUOTE_ALL,
                "index":False,
            }),
        ])

    def transform(self):     
        def contrib_comp_phone_fun(row):            
            if str(row['tele']) == "":
                return ""
            else:
                if str(row['tele_country']) != "":
                    if str(row['tele_area']) != "":
                        row = str(row['tele_country']) + '.' + \
                            str(row['tele_area']) + '.' + str(row['tele'])
                        return row
                    else:
                        row = str(row['tele_country']) + '.' + str(row['tele'])
                        return row
                else:
                    if str(row['tele_area']) != "":
                        row = str(row['tele_area']) + '.' + str(row['tele'])
                        return row
                    else:
                        row = str(row['tele'])
                        return row
        def contrib_comp_fax_fun(row):
            if str(row['fax']) == "":
                return ""
            else:
                if str(row['fax_country']) != "":
                    if str(row['fax_area']) != "":
                        row = str(row['fax_country']) + '.' + \
                            str(row['fax_area']) + '.' + str(row['fax'])
                        return row
                    else:
                        row = str(row['fax_country']) + '.' + str(row['fax'])
                        return row
                else:
                    if str(row['fax_area']) != "":
                        row = str(row['fax_area']) + '.' + str(row['fax'])
                        return row
                    else:
                        row = str(row['fax'])
                        return row
        self.index_mappings["csource"] = self.index_mappings[\
            self.index_mappings["csource"].str.strip().str.lower() == "frc"]
        merge_df = pd.merge(self.rep_contributors[(self.rep_contributors[\
            "b_client_visible"] != 0) | (self.rep_contributors[\
            "l_ent_visible"] != 0) | (self.rep_contributors[\
            "l_allow_contribution"] != 0) | (self.rep_contributors[\
            "internal_visible"] != 0)], 
            self.index_mappings,
            left_on = "c_contrib_id", right_on="ccompany_id")
        iconum_list = (merge_df['iconum'].unique()).tolist()
        merge_df = pd.merge(merge_df, self.fe_broker,
            left_on="n_fe_broker_id", right_on="n_broker_id", how="left")
        merge_df = merge_df[["iconum"
            ,"l_allow_contribution"
            ,"b_client_visible"
            ,"l_ent_visible"
            ,"internal_visible"
            ,"ccompany_name"
            ,"cfertype"
            ,"l_fer_external"
            ,"l_fer_internal"
            ,"n_fe_broker_id"
            ,"models_active"
            ,"models_internal"
            ,"models_external"
            ,"models_contributor_type"
            ,"models_link_back"
            ,"restriction_mode"
            ,"confidential"
            ,"fe_entitle"
            ,"entitle_feed"
            ,"firmlevelentitlement"
            ,"l_watermark"
            ,"l_linkback"]].drop_duplicates()
        merge_df = merge_df[merge_df["entitle_feed"] == 1]        
        merge_df =  pd.merge(merge_df, self.filermst, on = "iconum")
        merge_df =  pd.merge(merge_df, self.address, on = "iconum",
            how = "left")
        merge_df =  pd.merge(merge_df, self.inst, on = "address_id",
            how = "left")
        merge_df.ccompany_name = merge_df.ccompany_name.str.rstrip() 
        merge_df[["loc_str1", "loc_str2", "loc_str3"]] = \
            merge_df[["loc_str1", "loc_str2", "loc_str3"]].applymap(\
            lambda x: str(x).rstrip() if not pd.isna(x) else "")
        merge_df["loc_str1"] = merge_df["loc_str1"].apply(\
            lambda x: "" if str(x)=='*' else str(x))
        merge_df["b_client_visible"] = merge_df[\
            "b_client_visible"].fillna(0).astype(int)
        merge_df[["tele_country", "fax_country" , "tele_area", "fax_area"]] =\
            merge_df[["tele_country", "fax_country" , "tele_area",\
            "fax_area"]].replace(r'^\s+$', "#", regex=True)
        merge_df[["loc_city","loc_st_prov","loc_postal_code","email",\
            "models_contributor_type"]] = merge_df[["loc_city", "loc_st_prov",\
            "loc_postal_code", "email","models_contributor_type"]].applymap(
            lambda x: str(x).rstrip() if not pd.isna(x) else "")
        merge_df[["tele_country","tele_area","tele","fax_country","fax_area",\
            "fax",]] = merge_df[["tele_country","tele_area","tele",\
            "fax_country","fax_area","fax"]].applymap(
            lambda x: str(x).rstrip() if not pd.isna(x) else "")
        merge_df['contrib_comp_phone'] = merge_df.apply(\
            contrib_comp_phone_fun, axis=1)
        merge_df['contrib_comp_fax'] = merge_df.apply(\
            contrib_comp_fax_fun, axis=1)
        merge_df[["contrib_comp_phone", "contrib_comp_fax"]] = \
            merge_df[["contrib_comp_phone", "contrib_comp_fax"]].applymap(
                lambda x: str(x).replace('#','') if not pd.isna(x) else "")
        merge_df[["b_client_visible", "l_ent_visible","internal_visible",\
            "l_fer_external", "l_fer_internal", "models_active", 
            "models_internal", "models_external", "models_link_back",\
            "l_watermark", "fe_entitle"]] = merge_df[["b_client_visible",\
            "l_ent_visible","internal_visible", "l_fer_external",\
            "l_fer_internal", "models_active","models_internal",\
            "models_external", "models_link_back", "l_watermark",\
            "fe_entitle"]].applymap(lambda x: int(x) if not pd.isna(x) else "")
        merge_df["firmlevelentitlement"] = \
            merge_df["firmlevelentitlement"].apply(\
            lambda x: int(x) if not pd.isna(x) else "")
        merge_df = merge_df.rename(columns={
            "iconum_x": "contrib_comp_id",
            "ccompany_name": "contrib_comp_name",
            "loc_str1": "contrib_comp_address1",
            "loc_str2": "contrib_comp_address2",
            "loc_str3": "contrib_comp_address3",
            "loc_city": "contrib_comp_city",
            "loc_st_prov": "contrib_comp_state",
            "loc_postal_code": "contrib_comp_zip",
            "email": "contrib_comp",
            "l_ent_visible": "Entitlements_Visible",
            "cfertype": "Active_Status",
            "l_fer_external": "FER_External",
            "l_fer_internal": "FER_Internal",
            "n_fe_broker_id": "fe_broker_id",
            "internal_visible": "Internal_Visible"})
        merge_df = merge_df[[
            "contrib_comp_id"
            ,"contrib_comp_name"
            ,"contrib_comp_address1"
            ,"contrib_comp_address2"
            ,"contrib_comp_address3"
            ,"contrib_comp_city"
            ,"contrib_comp_state"
            ,"contrib_comp_zip"
            ,"contrib_comp"
            ,"contrib_comp_phone"
            ,"contrib_comp_fax"
            ,"b_client_visible"
            ,"Entitlements_Visible"
            ,"Internal_Visible"
            ,"Active_Status"
            ,"FER_External"
            ,"FER_Internal"
            ,"fe_broker_id"
            ,"models_active"
            ,"models_internal"
            ,"models_external"
            ,"models_contributor_type"
            ,"models_link_back"
            ,"restriction_mode"
            ,"confidential"
            ,"fe_entitle"
            ,"firmlevelentitlement"
            ,"l_watermark"
            ,"l_linkback"
            ,"address_id"
            ]].drop_duplicates()
        merge_df['Rank_row'] = merge_df.sort_values(['contrib_comp_id',
            'b_client_visible'],ascending=[False,False]).groupby([
            'contrib_comp_id','contrib_comp_address1']).cumcount() + 1
        merge_df1 = merge_df
        ######################################################################
        merge_df = pd.merge(self.rep_contributors[(self.rep_contributors[\
            "b_client_visible"] == 0) & (self.rep_contributors[\
            "l_ent_visible"] == 0) & (self.rep_contributors[\
            "l_allow_contribution"] == 0) & (\
            self.rep_contributors["internal_visible"] == 0) & (\
            self.rep_contributors["entitle_feed"] == 1)], self.index_mappings,
            left_on = "c_contrib_id", right_on = "ccompany_id")
        merge_df = pd.merge(merge_df, self.fe_broker,
            left_on="n_fe_broker_id",right_on="n_broker_id",how="left")
        merge_df = merge_df[["iconum"
            ,"l_allow_contribution"
            ,"b_client_visible"
            ,"l_ent_visible"
            ,"internal_visible"
            ,"ccompany_name"
            ,"cfertype"
            ,"l_fer_external"
            ,"l_fer_internal"
            ,"n_fe_broker_id"
            ,"models_active"
            ,"models_internal"
            ,"models_external"
            ,"models_contributor_type"
            ,"models_link_back"
            ,"restriction_mode"
            ,"confidential"
            ,"fe_entitle"
            ,"entitle_feed"
            ,"firmlevelentitlement"
            ,"l_watermark"
            ,"l_linkback"
            ]].drop_duplicates()
        merge_df = merge_df[~merge_df['iconum'].isin(iconum_list)]
        merge_df =  pd.merge(merge_df, self.filermst, on="iconum")
        merge_df =  pd.merge(merge_df, self.address, on="iconum", how="left")
        merge_df =  pd.merge(merge_df, self.inst, on="address_id")
        merge_df.ccompany_name = merge_df.ccompany_name.str.rstrip()  
        merge_df[["loc_str1", "loc_str2", "loc_str3"]] = \
            merge_df[["loc_str1", "loc_str2", "loc_str3"]].applymap(\
                lambda x: str(x).rstrip() if not pd.isna(x) else "")
        merge_df["loc_str1"] = \
            merge_df["loc_str1"].apply(lambda x: "" if str(x)=='*' else str(x))
        merge_df[["tele_country", "fax_country" , "tele_area", "fax_area"]] = \
            merge_df[["tele_country", "fax_country" , "tele_area",\
            "fax_area"]].replace(r'^\s+$', "#", regex=True)
        merge_df[["loc_city", "loc_st_prov", "loc_postal_code", "email",\
            "models_contributor_type"]] = merge_df[["loc_city","loc_st_prov",\
            "loc_postal_code", "email", "models_contributor_type"]].applymap(
            lambda x: str(x).rstrip() if not pd.isna(x) else "")		
        merge_df[["tele_country","tele_area","tele","fax_country",\
            "fax_area","fax"]] = merge_df[["tele_country","tele_area","tele",\
            "fax_country","fax_area","fax"]].applymap(
            lambda x: str(x).rstrip() if not pd.isna(x) else "")		
        merge_df['contrib_comp_phone'] = merge_df.apply(contrib_comp_phone_fun, axis=1)
        merge_df['contrib_comp_fax'] = merge_df.apply(contrib_comp_fax_fun, axis=1)
        merge_df[["contrib_comp_phone", "contrib_comp_fax"]] = \
            merge_df[["contrib_comp_phone", "contrib_comp_fax"]].applymap(
            lambda x: str(x).replace('#','') if not pd.isna(x) else "")
        merge_df[["b_client_visible","l_ent_visible","internal_visible",\
            "l_fer_external","l_fer_internal","models_active",\
            "models_internal","models_external","models_link_back",\
            "l_watermark","fe_entitle"]] = merge_df[["b_client_visible",\
            "l_ent_visible","internal_visible","l_fer_external",\
            "l_fer_internal","models_active","models_internal",\
            "models_external","models_link_back","l_watermark",\
            "fe_entitle"]].applymap(lambda x: int(x) if not pd.isna(x) else "")
        merge_df["firmlevelentitlement"] = \
            merge_df["firmlevelentitlement"].apply(\
            lambda x: int(x) if not pd.isna(x) else "")
        merge_df = merge_df.rename(columns={
            "iconum_x": "contrib_comp_id",
            "ccompany_name": "contrib_comp_name",
            "loc_str1": "contrib_comp_address1",
            "loc_str2": "contrib_comp_address2",
            "loc_str3": "contrib_comp_address3",
            "loc_city": "contrib_comp_city",
            "loc_st_prov": "contrib_comp_state",
            "loc_postal_code": "contrib_comp_zip",
            "email": "contrib_comp",
            "l_ent_visible": "Entitlements_Visible",
            "cfertype": "Active_Status",
            "l_fer_external": "FER_External",
            "l_fer_internal": "FER_Internal",
            "n_fe_broker_id": "fe_broker_id",
            "internal_visible": "Internal_Visible"})
        merge_df = merge_df[[
            "contrib_comp_id"
            ,"contrib_comp_name"
            ,"contrib_comp_address1"
            ,"contrib_comp_address2"
            ,"contrib_comp_address3"
            ,"contrib_comp_city"
            ,"contrib_comp_state"
            ,"contrib_comp_zip"
            ,"contrib_comp"
            ,"contrib_comp_phone"
            ,"contrib_comp_fax"
            ,"b_client_visible"
            ,"Entitlements_Visible"
            ,"Internal_Visible"
            ,"Active_Status"
            ,"FER_External"
            ,"FER_Internal"
            ,"fe_broker_id"
            ,"models_active"
            ,"models_internal"
            ,"models_external"
            ,"models_contributor_type"
            ,"models_link_back"
            ,"restriction_mode"
            ,"confidential"
            ,"fe_entitle"
            ,"firmlevelentitlement"
            ,"l_watermark"
            ,"l_linkback"]].drop_duplicates()
        # 1 AS Rank_row
        merge_df["Rank_row"] = 1
        merge_df = pd.concat([merge_df1, merge_df]).drop_duplicates(subset=[
            "contrib_comp_id"
            ,"contrib_comp_address1"
            ,"contrib_comp_address2"
            ,"contrib_comp_address3"
            ,"contrib_comp_city"
            ,"contrib_comp_state"
            ,"contrib_comp_zip"
            ,"contrib_comp"
            ,"contrib_comp_phone"
            ,"contrib_comp_fax"
            ,"b_client_visible"
            ,"Entitlements_Visible"
            ,"Internal_Visible"
            ,"Active_Status"
            ,"FER_External"
            ,"FER_Internal"
            ,"fe_broker_id"
            ,"models_active"
            ,"models_internal"
            ,"models_external"
            ,"models_contributor_type"
            ,"models_link_back"
            ,"restriction_mode"
            ,"confidential"
            ,"fe_entitle"
            ,"firmlevelentitlement"
            ,"l_watermark"
            ,"l_linkback"
        ])
        merge_df.sort_values(by = ['contrib_comp_id'], ascending=True)
        merge_df = merge_df[merge_df["Rank_row"] == 1]
        merge_df = merge_df[[
            "contrib_comp_id"
            ,"contrib_comp_name"
            ,"contrib_comp_address1"
            ,"contrib_comp_address2"
            ,"contrib_comp_address3"
            ,"contrib_comp_city"
            ,"contrib_comp_state"
            ,"contrib_comp_zip"
            ,"contrib_comp"
            ,"contrib_comp_phone"
            ,"contrib_comp_fax"
            ,"b_client_visible"
            ,"Entitlements_Visible"
            ,"Internal_Visible"
            ,"Active_Status"
            ,"FER_External"
            ,"FER_Internal"
            ,"fe_broker_id"
            ,"models_active"
            ,"models_internal"
            ,"models_external"
            ,"models_contributor_type"
            ,"models_link_back"
            ,"restriction_mode"
            ,"confidential"
            ,"fe_entitle"
            ,"firmlevelentitlement"
            ,"l_watermark"
            ,"l_linkback"]].drop_duplicates()
        merge_df[["Active_Status","models_contributor_type"]] = \
            merge_df[["Active_Status", "models_contributor_type"]].applymap(
                lambda x: str(x).rstrip() if not pd.isna(x) else "")
        merge_df[["contrib_comp_id","contrib_comp_name",
            "contrib_comp_address1","contrib_comp_address2",
            "contrib_comp_address3","contrib_comp_city",
            "contrib_comp_state","contrib_comp_zip","contrib_comp",
            "contrib_comp_phone","contrib_comp_fax","b_client_visible",
            "Entitlements_Visible","Internal_Visible","Active_Status",
            "FER_External","FER_Internal","fe_broker_id",
            "models_active","models_internal","models_external",
            "models_contributor_type","models_link_back","restriction_mode",
            "confidential","fe_entitle","firmlevelentitlement","l_watermark",
            "l_linkback"]] = merge_df[["contrib_comp_id","contrib_comp_name",
            "contrib_comp_address1","contrib_comp_address2",
            "contrib_comp_address3","contrib_comp_city",
            "contrib_comp_state","contrib_comp_zip","contrib_comp",
            "contrib_comp_phone","contrib_comp_fax","b_client_visible",
            "Entitlements_Visible","Internal_Visible","Active_Status",
            "FER_External","FER_Internal","fe_broker_id","models_active",
            "models_internal","models_external","models_contributor_type",
            "models_link_back","restriction_mode","confidential","fe_entitle",
            "firmlevelentitlement","l_watermark","l_linkback"]].applymap(\
            lambda x : '' if str(x).lower()=="nan" else str(x).strip())
        try:
            self.ac_contributors = merge_df
            del merge_df
            del merge_df1
            gc.collect()
        except Exception:
            logger.write_dbg('{}'.format(utilities.capture_trace()))
            raise
        if any(self.results):
            for df_name, file_name in self.results.items():
                setattr(self, df_name, self.clean_text_column(getattr(self,\
                df_name)))
        else:
            logger.write_dbg('Empty Dictionary: {}'.format(\
                self.current_filename))
            raise ValueError       
    def clean_text_column(self, cleaned_df):
        types = cleaned_df.apply(lambda x: pd.lib.infer_dtype(x.values))
        control_chars = ''.join(map(chr, list(range(0, 9)) + \
            list(range(11, 13)) + list(range(14, 32)) + \
            list(range(128, 130)) + list(range(140, 145)) + \
            list(range(156, 158))))
        control_char_re = re.compile('[%s]' % re.escape(control_chars))
        for key, value in types.items():
            value = str(value)
            key = str(key)
            if value == 'mixed' or value == 'unicode' or value == 'string':
                try:
                    if "date" not in key.lower():
                        logger.write_log('triming: {}'.format(key))
                        cleaned_df[key] = cleaned_df[key].apply(\
                            lambda x: control_char_re.sub(\
                            '', x) if not x is None else x).str.strip(\
                            u'\u007F\u0009 ').str.replace('\n', '\\n')
                except:
                    logger.write_dbg('{}'.format(utilities.capture_trace()))
                    raise
        return cleaned_df