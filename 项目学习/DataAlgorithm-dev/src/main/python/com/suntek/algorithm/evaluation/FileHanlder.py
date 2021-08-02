import os
import pandas as pd
from typing import List


class FileHanlder():

    def list_excel_dir(self, dir_path):
        self.dir_path = dir_path
        # 遍历目录
        excels_file = [i for i in os.listdir(self.dir_path) if i.endswith('.xlsx')]
        excels_path = [os.path.join(self.dir_path, i) for i in excels_file]

        # 保存该路径下所有的 excel 文件名的 列表
        self.excels_file = excels_file
        # 保存该路径下所有的 excel 文件名的 相对路径， './source_datas/表格名 '
        self.excels_path = excels_path

        return excels_file, excels_path

    def single_excel_read(self, excel_path):
        """
        :param excel_path: 一张 excel表的 路径
        :return: excel 的 {sheet:dict} 数据 或者 单个DataFrame数据
        """
        io = pd.io.excel.ExcelFile(excel_path)
        content_tmp = pd.read_excel(io, sheet_name=None)
        # 判断源excel是否有多个 sheet
        if isinstance(content_tmp, dict):
            # 保存为 dict 数据
            self.content_dict = content_tmp
        else:
            # 保存为 DataFrame 数据
            self.content_df = content_tmp
        return content_tmp

    def single_excel_save(self, single_excel_sheets, excel_name, report_dir_path, sheet_names=None):
        pass

    def multi_excels_read(self):
        pass

    def multi_excels_save(self, multi_excel_sheets, excel_name, report_dir_path, sheet_names):
        writer = pd.ExcelWriter(report_dir_path + "/" + excel_name, engine='openpyxl')
        num = 1
        for sheet_name in sheet_names:
            # 下面的保存文件处填写writer，结果会不断地新增sheet，避免循环时被覆盖
            multi_excel_sheets[sheet_name].to_excel(excel_writer=writer, sheet_name=sheet_name, encoding="utf-8",
                                                    index=False)
            print(sheet_name + "  保存成功！共%d个，第%d个。" % (len(sheet_names), num))
            num += 1
        writer.save()
        writer.close()

    def list_dir_all_files(self, source_dir):
        """
        给需要遍历的 目录路径，返回 所有文件的路径的列表 及 此文件所在目录的路径列表，俩个位子一一对应
        :param source_dir:
        :return:
        """
        father_dir_paths = []
        file_paths = []
        for root, dirs, files in os.walk(source_dir):
            for file in files:
                # 获取文件所属目录
                father_dir_paths.append(root)
                # 获取文件路径
                file_paths.append(os.path.join(root, file))
        return file_paths, father_dir_paths

    def data_preprocess(self, dataframe):

        return dataframe.dropna()

    @staticmethod
    def panda_chart(df_list: List[pd.DataFrame]
                    , cols: List, sheets: List, title_x: str, title_y: str, chart_type='scatter', file_path="")  :
        """
        可以插入多个sheet，并进行绘图，可以挑选绘图的格式

        :param df_list:
        :param cols:
        :param sheets:
        :param title_x:
        :param title_y:
        :param chart_type:
        :param file_path:
        :return:
        """

        writer = pd.ExcelWriter(file_path, engine='xlsxwriter')
        for i, df in enumerate(df_list):
            sheet_name = f'{sheets[i]}'
            df.to_excel(writer, sheet_name=sheet_name, index=False)

            workbook = writer.book
            worksheet = writer.sheets[sheet_name]
            chart = workbook.add_chart({'type': chart_type})
            # set colors for the chart each type .
            colors = ['#E41A1C', '#377EB8', '#4DAF4A', '#984EA3', '#FF7F00']
            # Configure the series of the chart from the dataframe data.
            for col_num in range(1, cols + 1):
                chart.add_series({
                    'name': [f'{sheet_name}', 0, col_num],
                    'categories': [f'{sheet_name}', 1, 0, df.shape[0], 0],
                    # axis_x start row ,start col,end row ,end col
                    'values': [f'{sheet_name}', 1, col_num, df.shape[0], col_num],  # axis_y value of
                    'fill': {'color': colors[col_num - 1]},  # each type color choose
                    'overlap': -10,
                })

            # Configure the chart axes.
            chart.set_x_axis({'name': f'{title_x}'})
            chart.set_y_axis({'name': f'{title_y}', 'major_gridlines': {'visible': False}})
            chart.set_size({'width': 900, 'height': 400})
            # Insert the chart into the worksheet.
            worksheet.insert_chart(row=1,col=6, chart=chart)
        writer.save()
