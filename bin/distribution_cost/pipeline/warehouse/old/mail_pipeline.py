import logging
import datetime
import pandas as pd

from distribution_cost.configuration import app, mails


def send_mail(current_date=datetime.datetime.now()):

    app_config = app.AppConfig()

    # Initialise mailer
    mailer = mails.Mailer(host=app_config._mailing_conn_dict["host"],
                          sender=app_config._mailing_conn_dict["sender"],
                          user=app_config._mailing_conn_dict["user"],
                          password=app_config._mailing_conn_dict["passwd"],
                          topic='',
                          receivers=app_config._mailing_conn_dict["receivers"])

    # Send email
    logging.info('Sending email')
    current_date = pd.Timestamp(datetime.date.today())
    mailer.send(
        '[Test mail] {}'.format(current_date.strftime('%Y-%m-%d')),
        'Hello, \
        \n\nThis is a test mail. \
        \n\nSincerely')


if __name__ == '__main__':
    send_mail()
