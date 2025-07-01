import os

from fpdf import FPDF
from scrapy.utils.project import get_project_settings


def log_parse_file(task_id, client_id, source):
    dir_path_for_open = get_project_settings()["LOG_DIR"] + client_id + '/' + source + '/'
    with open(dir_path_for_open + '/scrapy.log', encoding='utf-8') as f1:
        dir_path_for_parse_log = get_project_settings()["PARSE_LOG_DIR"] + client_id + '/' + source
        if not os.path.exists(dir_path_for_parse_log):
            os.makedirs(dir_path_for_parse_log)

        pdf = FPDF()
        pdf.add_page()
        pdf.set_font('Arial', size=12)
        lines = f1.readlines()
        for line in lines:
            if line.__contains__("<200"):
                pdf.write(5, line)
                pdf.ln()
        pdf.output(os.path.join(dir_path_for_parse_log, f'{task_id}.pdf'), 'F')
