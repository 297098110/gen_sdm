目录结构：
    - gen sdm
        - ldm [放置源系统分析字段级分析excel]
        - pdm [放置物理模型基础模型excel]
        - log [日志目录]
        - template [模板目录]
        - gen_sdm.py 程序脚本
        - result_sdm.xlsx 程序运行后生成的 SDM excel

注意：
    1. 源系统分析字段级分析excel 放入 ldm 目录下
    2. 物理模型基础模型excel 放入 pdm 目录下
    3. ldm 和 pdm 目录只支持单个文件，请删除其余不想关文件
    4. 源系统字段级分析excel第一行应为空或写入"字段级分析", sheet工作表名称应为 "字段级分析"
    5. 物理模型基础模型excel sheet工作表名称应为 "01_表级分析"和 "02_字段级分析"，第一行应为列头，不能为空或其他
    4-5 详见 template目录下模板
    6. || 规则中非变量需要用单引号引起，字段需加上 T1.
        exp: 'PLN' || T1.CUSTR_NBR || '表级编号'
