#from databricks.sdk import WorkspaceClient
#import os
#from datetime import timedelta
#
#host = os.environ["DBT_HOST"]
#if host is None or os.environ["DBT_TOKEN"] is None or os.environ["JOB_ID"] is None:
#    RuntimeError("Credentials not given")
#client = WorkspaceClient(
#    host=host if host.startswith("https://") else f"https://{host}",
#    token=os.environ["DBT_TOKEN"],
#)
#run = client.jobs.run_now(job_id=int(os.environ["JOB_ID"])).result(timeout=timedelta(minutes=30))
#print(run.state.result_state)


import smtplib
import os

sender = "Private Person <hello@demomailtrap.co>"
receiver = "A Test User <richardwaleawe@gmail.com>"
password = os.environ["SMTP_PASSWORD"]

message = f"""\
Subject: Hi Mailtrap
To: {receiver}
From: {sender}

This is a test e-mail message."""

with smtplib.SMTP("live.smtp.mailtrap.io", 587) as server:
    server.starttls()
    server.login("api", password)
    server.sendmail(sender, receiver, message)