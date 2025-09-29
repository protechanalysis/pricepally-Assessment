import smtplib
import ssl
from email.message import EmailMessage

from airflow.models import Variable


def task_fail_alert(context):
    """
    This function send notification of a failed dags task
    """
    state = context.get("task_instance").state
    dag = context.get("task_instance").dag_id
    task = context.get("task_instance").task_id
    exec_date = context.get("task_instance").start_date
    log = context.get("task_instance").log_url
    dag_owner = context["params"].get("dag_owner")

    email_sender = Variable.get('email_sender')
    email_password = Variable.get('email_password')
    email_receiver = Variable.get('email_receiver')

    print(context)
    subject = f"task {task} of {state} in dag {dag} failed"
    body = f'''
    Hey {dag_owner}

    The task {task} in dag {dag} has failed for run date {exec_date}

    Here is the log url: {log}
    '''
    em = EmailMessage()
    em['From'] = email_sender
    em['To'] = email_receiver
    em['Subject'] = subject
    em.set_content(body)

    context = ssl.create_default_context()

    with smtplib.SMTP_SSL('SMTP.gmail.com', 465, context=context) as smtp:
        smtp.login(email_sender, email_password)
        smtp.sendmail(email_sender, email_receiver, em.as_string())
        print("Email Sent Successfully")