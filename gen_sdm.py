# -*- coding: utf-8 -*-
import os
import re
import sys
import copy
import time
import xlrd
import logging
import warnings
import numpy as np
import pandas as pd

warnings.simplefilter(action="ignore")

CURRENT_PATH = os.getcwd()

# 着色字段
COLOR_LIST = [
    "数据来源表编号",
    "ETL任务名",
    "目标字段赋值规则",
    "概要映射规则注释",
    "JOIN方式",
    "次源表库名",
    "次源表英文名",
    "次源表别名",
    "次源表中文名",
    "JOIN条件",
    "WHERE条件",
    "备注"
]

# 技术字段
TEC_FIELD = [
    "Create_Dt",
    "Update_Dt",
    "Start_Dt",
    "End_Dt",
    "Id_Mark",
    "Src_Tab",
    "Etl_Timestamp",
    "Job_Cd",
    "Part_Id",
]

# 日志输出等级
DEFAULT_LOG_LEVEL = logging.INFO
# 默认日志格式
DEFAULT_LOG_FMT = "%(asctime)s %(filename)s [line:%(lineno)d] %(levelname)s: %(message)s"
# 默认时间格式
DEFAULT_LOG_DATEFMT = "%Y-%m-%d %H:%M:%S"
# 默认日志文件名称
DEFAULT_LOG_FILE = time.strftime("%Y%m%d", time.localtime(time.time()))
# 默认日志存放路径
DEFAULT_LOG_DIR = "."
# 项目日志
DEFAULT_LOG_FILENAME = DEFAULT_LOG_DIR + "/log/" + DEFAULT_LOG_FILE + ".log"


class Logger:
    def __init__(self):
        self._logger = logging.getLogger()
        self.formatter = logging.Formatter(fmt=DEFAULT_LOG_FMT, datefmt=DEFAULT_LOG_DATEFMT)
        if not self._logger.handlers:
            self._logger.addHandler(self._get_file_handler(DEFAULT_LOG_FILENAME))
            self._logger.addHandler(self._get_console_hander())
            self._logger.setLevel(DEFAULT_LOG_LEVEL)
            self.base_dir = os.path.dirname(os.getcwd())

    def _get_file_handler(self, filename):
        try:
            filehandler = logging.FileHandler(filename=filename, encoding="utf-8")
        except FileNotFoundError:
            os.mkdir(DEFAULT_LOG_DIR + "/log")
            filehandler = logging.FileHandler(filename=filename, encoding="utf-8")
        filehandler.setFormatter(self.formatter)
        return filehandler

    def _get_console_hander(self):
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(self.formatter)
        return console_handler

    @property
    def logger(self):
        return self._logger


class DataMigration:
    def __init__(self):
        self.result_columns = [
            "组号", "目标表英文名", "目标字段英文名", "序号", "数据来源表编号", "目标表库名", "目标表中文名", "目标字段中文名",
            "目标字段数据类型", "主键", "ETL任务", "目标字段赋值规则", "概要映射规则注释", "主源字段英文名", "主源字段中文名",
            "主源字段数据类型", "UI", "主源表库名", "主源表英文名", "主源表别名", "主源表中文名", "JOIN方式", "次源表库名",
            "次源表英文名", "次源表别名", "次源表中文名", "JOIN条件", "是否用全量数据加载[Y/空]", "WHERE条件", "GROUP BY列表",
            "T13变种算法中的删除加工", "组级自定义SQL", "是否手工添加组标志", "备注", "修改关注"
        ]
        __first_data = [np.nan for i in range(len(self.result_columns))]
        __first_data[0] = "SDM模板"
        self.result_data = pd.DataFrame(data=[__first_data, self.result_columns], columns=self.result_columns)
        self.logger = Logger().logger
        self.alg_dict = None

    @staticmethod
    def high_light(x):
        y = x.isin(COLOR_LIST)
        return ["background-color: yellow" if z else "" for z in y]

    @staticmethod
    def get_file(file_type):
        for root, dirs, files in os.walk("/".join([CURRENT_PATH, file_type])):
            return "/".join([root, files[0]])

    @staticmethod
    def pre_pdm_t_operate(pdm_t_data):
        """
        pdm 表级信息预处理
        :param pdm_t_data:
        :return:
        """
        # 过滤出有算法的
        pdm_t_data = pdm_t_data[pdm_t_data.算法类型.notnull()]
        pdm_t_data = pdm_t_data.apply(lambda x: x.str.strip(), axis=1)

        pdm_dict = dict()
        pdm_t_list = pdm_t_data.to_dict(orient="records")
        for t_d in pdm_t_list:
            pdm_dict[t_d["表中文名"]] = t_d["算法类型"]
        return pdm_dict

    @staticmethod
    def pre_pdm_f_operate(pdm_f_data):
        """
        pdm 字段级信息预处理
        :param pdm_f_data:
        :return:
        """
        # 删除技术字段
        pdm_f_data = pdm_f_data[~pdm_f_data["字段英文名"].isin(TEC_FIELD)]
        return pdm_f_data

    def pre_ldm_operate(self, ldm_data):
        """
        ldm 预处理
        :param ldm_data:
        :return:
        """
        del ldm_data["字段序号"]
        ldm_data = ldm_data.apply(lambda x: x.str.strip(), axis=1)
        # 过滤出是否入整合模型层标志为 "Y" 的数据
        ldm_data = ldm_data[ldm_data["是否入整合模型层标志"].isin(["Y"])]
        # 过滤出以下字段不为空和不为" "的数据
        complie_list = [np.nan, " "]
        true_data = ldm_data[
            (~ldm_data["整合模型层LDM表中文名"].isin(complie_list)) &
            (~ldm_data["整合模型层LDM表字段中文名"].isin(complie_list)) &
            (~ldm_data["整合模型层映射规则描述"].isin(complie_list))
        ]

        # 空值和 " " 的数据输出在日志
        false_data = pd.DataFrame()
        for field in ["整合模型层LDM表中文名", "整合模型层LDM表字段中文名", "整合模型层映射规则描述"]:
            data = ldm_data[(ldm_data[field].isin(complie_list))]
            false_data = pd.concat([false_data, data])
        if not false_data.empty:
            self.logger.warning(f"字段存在空值：\n{false_data}")
            raise Exception("整合模型层LDM表中文名/整合模型层LDM表字段中文名/整合模型层映射规则描述存在空值，请检查")

    def make_float_default(self, x):
        try:
            a = re.findall(r"DECIMAL\(\d+ ?, ?(\d)\)", x)
        except TypeError:
            self.logger.error("pdm 字段类型为空，请检查")
        else:
            if a:
                base_float = ["0."]
                base_float.extend(["0" for i in range(int(a[0]))])
                return "".join(base_float)
            else:
                return ""

    @staticmethod
    def slice_rule(x):
        x_list = x.split("||")
        x_str = ", ".join(x_list)
        y_home = "CASE WHEN "
        y_mid = " ELSE CONCAT("
        y_end = ") END"
        coa_list = [f"COALESCE({i}, '') = '' THEN ''" for i in x_list]
        coa_str = ", ".join(coa_list)
        y = "".join([y_home, coa_str, y_mid, x_str, y_end])
        return y

    @staticmethod
    def split_rule(x):
        try:
            a = re.findall(r"【(\w+)】(.+)", x["概要映射规则注释"])
        except TypeError:
            return x["概要映射规则注释"]
        else:
            if a:
                for i in a:
                    if i[0] == x["目标字段中文名"]:
                        return i[1]
                return x["概要映射规则注释"]
            else:
                return x["概要映射规则注释"]

    def rule_assign(self, data):
        """
        规则赋值
        :param data:
        :return:
        """
        # 直接映射
        # VARCHAR
        data.loc[(data.for_split.isin(["直接映射"]) &
                  (data.目标字段数据类型.str.contains("VARCHAR"))), "目标字段赋值规则"] = \
            data["主源表别名"] + "." + data["主源字段英文名"]
        # INTEGER
        data.loc[(data.for_split.isin(["直接映射"]) &
                  (data.目标字段数据类型.str.contains("INTEGER"))), "目标字段赋值规则"] = \
            "COALESCE(CAST(" + data["主源表别名"] + "." + data["主源字段英文名"] + " AS " + \
            data["目标字段数据类型"] + "), 0)"
        # DECIMAL
        data.loc[(data.for_split.isin(["直接映射"]) &
                  (data.目标字段数据类型.str.contains("DECIMAL"))), "目标字段赋值规则"] = \
            "COALESCE(CAST(" + data["主源表别名"] + "." + data["主源字段英文名"] + " AS " + \
            data["目标字段数据类型"] + "), " + data["目标字段数据类型"].map(self.make_float_default) + ")"
        # DATE/TIMESTAMP
        data.loc[(data.for_split.isin(["直接映射"]) &
                  (data.目标字段数据类型.str.contains("DATE|TIMESTAMP"))), "目标字段赋值规则"] = \
            "COALESCE(TO_TIMESTAMP(REGEXP_REPLACE(" + data["主源表别名"] + "." + data["主源字段英文名"] + \
            ", '/|_|-|', ''), 'yyyyMMdd HH:mm:ss.SSS'),\nTO_TIMESTAMP(REGEXP_REPLACE(" + \
            data["主源表别名"] + "." + data["主源字段英文名"] + \
            ", '/|_|-|', ''), 'yyyyMMdd HH:mm:ss'),\nTO_TIMESTAMP(REGEXP_REPLACE(" + \
            data["主源表别名"] + "." + data["主源字段英文名"] + \
            ", '/|_|-|', ''), 'yyyyMMdd'),\nTO_TIMESTAMP(REGEXP_REPLACE(" + \
            data["主源表别名"] + "." + data["主源字段英文名"] + \
            ", '/|_|-|', ''), 'HH:mm:ss'), TO_TIMESTAMP('${min_date}', 'yyyyMMdd'))"

        # 标志映射
        data.loc[(data.for_split.isin(["标志映射"])), "目标字段赋值规则"] = \
            "CASE WHEN " + data["主源表别名"] + "." + data["主源字段英文名"] + \
            " = '' THEN \'Y\'\nWHEN " + data["主源表别名"] + "." + data["主源字段英文名"] + \
            " = '' THEN \'N\'\nELSE CONCAT(\'~\', " + data["主源表别名"] + "." + data["主源字段英文名"] + \
            ", '')\nEND"
        # 代码映射
        data.loc[(data.for_split.isin(["代码映射"])), "目标字段赋值规则"] = \
            "<" + data["主源表英文名"] + ">" + "<" + data["主源字段英文名"] + ">" + \
            "<" + data["主源表别名"] + ">" + "<" + data["目标表英文名"] + ">" + \
            "<" + data["目标字段英文名"] + ">"

        # || CASE WHEN COALESCE(T1.OPENBANKNO, '') = '' THEN '' ELSE CONCAT('SYMIO', T1.OPENBANKNO) END
        data.loc[(data.for_split.fillna("").str.contains("\|\|")), "目标子弹赋值规则"] = \
            data.loc[(data.for_split.fillna("").str.contains("\|\|"))]["for_split"].apply(self.slice_rule)

        del data["for_split"]
        return data

    def data_align(self, data):
        """
        数据对齐
        :param data:
        :return:
        """
        result_data = pd.DataFrame(columns=self.result_columns)
        result_data["组号"] = data["组号"]
        result_data.fillna({"组号": data["源系统表名"] + "-" + data["表英文名"]}, inplace=True)
        result_data["目标表英文名"] = data["表英文名"]
        result_data["目标字段英文名"] = data["字段英文名"]
        result_data["序号"] = data["字段序号"]
        result_data["数据来源表编号"] = "1"
        result_data["目标表库名"] = "${iml_schema}"
        result_data["目标表中文名"] = data["表中文名"]
        result_data["目标字段中文名"] = data["字段中文名"]
        result_data["目标字段数据类型"] = data["字段类型"]
        result_data["主键"] = data["主键"]
        result_data["ETL任务"] = data["源系统标识名"].str.lower() + "0200"
        result_data["概要映射规则注释"] = data["整合模型层映射规则描述"]
        result_data["主源字段英文名"] = data["源系统字段名"]
        result_data["主源字段中文名"] = data["源字段中文名"]
        result_data["主源字段数据类型"] = data["源系统数据类型"]
        result_data["UI"] = data["唯一索引或主键字段[UI1,UI2/空]"]
        result_data["主源表库名"] = "${iol_schema}"
        result_data["主源表英文名"] = data["数据平台源表主干名"]
        result_data["主源表英文名"].fillna(method="ffill", inplace=True)
        result_data["主源表英文名"].fillna(method="backfill", inplace=True)
        result_data["主源表别名"] = data["主源表别名"]
        result_data["主源表别名"].fillna(method="ffill", inplace=True)
        result_data["主源表别名"].fillna(method="backfill", inplace=True)
        result_data["主源表中文名"] = data["源表中文名"]
        result_data["备注"] = data["问题/备注"]
        result_data["备注"].fillna(method="ffill", inplace=True)
        result_data["备注"].fillna(method="backfill", inplace=True)
        result_data.sort_values(by=["序号"], inplace=True)
        return result_data

    def shuffle(self, table_name, ldm_data, pdm_data):
        pdm_index = pdm_data.size().index
        direct_list = ldm_data["整合模型层LDM表中文名"].str.split("/", expand=True)
        direct_f_list = ldm_data["整合模型层LDM字段中文名"].str.split("/", expand=True)
        # 检查整合模型层LDM字段是否正确
        if direct_list.shape[1] != direct_f_list.shape[1]:
            self.logger.warning(f"源系统表名\"{table_name}\"的数据\"整合模型层LDM\"相关字段不正确，请检查.")
            return
        fn_data = pd.DataFrame()
        for i in range(direct_list.shape[1]):
            table_verify = direct_list[i].notnull()
            field_verify = direct_f_list[i].notnull()
            tv = table_verify.values == field_verify.values
            if False in tv:
                self.logger.warning(f"源系统表名\"{table_name}\"的数据\"整合模型层LDM\"相关字段不正确，请检查.")
                return
            cp_data = copy.deepcopy(ldm_data)
            cp_data["整合模型层LDM表中文名"] = direct_list[i]
            cp_data["整合模型层LDM字段中文名"] = direct_f_list[i]
            fn_data = pd.concat([fn_data, cp_data], axis=0)
        fn_data.dropna(subset=["整合模型层LDM表中文名"], inplace=True)

        fn_group = fn_data.groupby("整合模型层LDM表中文名")
        for each in fn_group:
            if each[0] not in pdm_index:
                self.logger.info(f"模型层LDM表 \"{each[0]}\" 不存在于pdm中")
                continue
            i_list = list()
            copy_each_data = copy.deepcopy(each[1])
            pdm_df = pdm_data.get_group(each[0])
            group_row = list()
            common_row = list()
            # 判断对应 LDM 字段是否重复，如果重复需分组
            p_data = copy_each_data.groupby("整合模型层LDM字段中文名").size()
            com_list = p_data[p_data.values == 1].index.to_list()
            if np.nan in com_list:
                com_list.remove(np.nan)
            for r_index, row in copy_each_data.iterrows():
                # 不重复的行，每组都要携带
                if row["整合模型层LDM字段中文名"] in com_list:
                    for j in row["整合模型层LDM字段中文名"].split("&"):
                        td = copy.deepcopy(row)
                        td["整合模型层LDM字段中文名"] = j
                        common_row.append(td)
                # 重复的行，作为分组依据
                else:
                    rep_list = list()
                    try:
                        for j in row["整合模型层LDM字段中文名"].split("&"):
                            td = copy.deepcopy(row)
                            td["整合模型层LDM字段中文名"] = j
                            rep_list.append(td)
                    except AttributeError:
                        self.logger.warning(
                            f"缺少整合模型层LDM字段中文名：\n"
                            f"源系统表名：{row['源系统表名']}；\n"
                            f"源系统字段名：{row['源系统字段名']}；\n"
                            f"整合模型层LDM字段中文名：{row['整合模型层LDM字段中文名']}"
                        )
                        continue
                    group_row.append(rep_list)
            # 无重复，不需分组
            if not group_row:
                if not common_row:
                    continue
                i_list.append(common_row)
            # 重复，需分组
            else:
                for g_index, group in enumerate(group_row):
                    group.extend(common_row)
                    new_group = list()
                    for e in group:
                        ee_ch = copy.deepcopy(each)
                        ee_ch["组号"] = str(g_index)
                        new_group.append(ee_ch)
                    i_list.append(new_group)
            if not i_list:
                continue
            else:
                col_name = pdm_df.columns.to_list()
                col_name.extend(["整合模型层LDM表中文名", "整合模型层LDM字段中文名"])
                pdm_df = pdm_df.reindex(columns=col_name)
                pdm_df.loc[:, ["整合模型层LDM表中文名"]] = pdm_df["表中文名"]
                pdm_df.loc[:, ["整合模型层LDM字段中文名"]] = pdm_df["字段中文名"]
                for i in i_list:
                    copy_data = pd.DataFrame(i)
                    # 数据合并
                    merge_data = pd.merge(copy_data, pdm_df, how="right",
                                          on=["整合模型层LDM表中文名", "整合模型层LDM字段中文名"])
                    merge_data["源系统表名"].fillna(method="ffill", inplace=True)
                    merge_data["源系统表名"].fillna(method="backfill", inplace=True)
                    merge_data["源表中文名"].fillna(method="ffill", inplace=True)
                    merge_data["源表中文名"].fillna(method="backfill", inplace=True)
                    merge_data["表英文名"].fillna(method="ffill", inplace=True)
                    merge_data["表英文名"].fillna(method="backfill", inplace=True)
                    if "组号" in merge_data.keys():
                        merge_data["组号"] = merge_data["源系统表名"] + "-" + merge_data["表英文名"] + \
                                            "-" + merge_data["组号"]
                    else:
                        merge_data["组号"] = merge_data["源系统表名"] + "-" + merge_data["表英文名"]
                    merge_data["组号"].fillna(method="ffill", inplace=True)
                    merge_data["组号"].fillna(method="backfill", inplace=True)
                    merge_data["主源表别名"] = "T1"
                    merge_data = merge_data.sort_values(by=["组号", "序号"])
                    merge_data = merge_data.reset_index(drop=True)
                    result_data = self.data_align(merge_data)
                    result_data["for_split"] = result_data[["概要映射规则注释", "目标字段中文名"]].apply(self.split_rule, axis=1)
                    fina_data = self.rule_assign(result_data)
                    self.result_data = pd.concat([self.result_data, fina_data], axis=0,
                                                 ignore_index=True, sort=False)

    def data_shuffle(self, src_data, pdm_t_data, pdm_f_data):
        """
        数据整理
        :param src_data: EDW 数据
        :param pdm_t_data: PDM 表级信息
        :param pdm_f_data: PDM 字段级信息
        :return:
        """
        self.alg_dict = self.pre_pdm_t_operate(pdm_t_data)
        ldm_data = self.pre_ldm_operate(src_data)
        pdm_f_data = self.pre_pdm_f_operate(pdm_f_data)
        ldm_group = ldm_data.groupby("源系统表名")
        pdm_group = pdm_f_data.groupby("表中文名")
        for ldm_each in ldm_group:
            self.shuffle(ldm_each[0], ldm_each[1], pdm_group)

    def main(self):
        """
        主函数
        :return:
        """
        # 读取 Excel
        ldm_file = self.get_file("ldm")
        ldm_df = pd.read_excel(io=ldm_file, sheet_name="字段级分析", header=1)
        pdm_file = self.get_file("pdm")
        pdm_wk = xlrd.open_workbook(filename=pdm_file)
        pdm_t_df = pd.read_excel(io=pdm_wk, sheet_name="01_表级信息")
        pdm_f_df = pd.read_excel(io=pdm_wk, sheet_name="02_字段级信息")
        # 数据整理
        self.data_shuffle(src_data=ldm_df, pdm_t_data=pdm_t_df, pdm_f_data=pdm_f_df)
        # 输出 Excel
        self.result_data = self.result_data.style.apply(self.high_light)
        self.result_data.to_excel("./result_sdm.xlsx", sheet_name="SDM", index=False, header=False)


if __name__ == '__main__':
    script = DataMigration()
    script.main()
