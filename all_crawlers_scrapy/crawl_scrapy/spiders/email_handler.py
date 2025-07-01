import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from scrapy.utils.project import get_project_settings


def send_email(email, subject, additional_info, body):
    _login = 'mfi.admin@setuserv.com'
    _password = "AlphaSetuNo12#"
    _from = "MFI Admin<mfi.admin@setuserv.com>"
    _to = email
    _cc_to = get_project_settings()['CC_EMAIL']
    msg = MIMEMultipart()
    msg['From'] = _from
    msg['To'] = _to
    msg['CC'] = _cc_to
    msg['Subject'] = subject
    msg.attach(MIMEText(additional_info, 'html'))
    msg.attach(MIMEText(body, 'html'))
    s = smtplib.SMTP('smtp.gmail.com', 587)
    s.starttls()
    s.login(_login, _password)
    text = msg.as_string()
    s.sendmail(_from, [_to, _cc_to], text)
    s.quit()

def send_email_with_file(email, subject, body, files):
    _login = 'mfi.admin@setuserv.com'
    _password = 'AlphaSetuNo12#'
    _from = "MFI Admin<mfi.admin@setuserv.com>"
    _to = email
    _cc_to = get_project_settings()['CC_EMAIL']
    msg = MIMEMultipart()
    msg['From'] = _from
    msg['To'] = _to
    msg['CC'] = _cc_to
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'html'))
    for file in files:
        attachment = open(file, "rb")
        _file = MIMEBase('application', 'octet-stream')
        _file.set_payload(attachment.read())
        encoders.encode_base64(_file)
        _file.add_header('Content-Disposition', f"attachment; filename={file}")
        msg.attach(_file)

    s = smtplib.SMTP('smtp.gmail.com', 587)
    s.starttls()
    s.login(_login, _password)
    text = msg.as_string()
    s.sendmail(_from, [_to, _cc_to], text)
    print('Mail is sent')
    s.quit()
