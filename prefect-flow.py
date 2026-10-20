"""DS 3022 Data Project 2: Prefect flow.

Your pipeline must, without human intervention:
  1. POST to the scatter API to populate your queue (once per run, never inside a retry loop)
  2. Monitor the queue (get_queue_attributes) and collect all 21 messages
  3. Land each fragment in DuckDB (raw.fragments) BEFORE deleting its message
  4. Run dbt: tests are the quality gate, the mart model holds the phrase
  5. Submit the phrase to the 'dp2-submit' queue and log the HTTP status code

Run:   python prefect-flow.py
Then:  python kit/check_submission.py

Every task already has a header. Checkpoint A (Lesson 9): fill in populate_queue,
get_counts and monitor_queue. Lesson 10: collect_messages, dbt_build, read_phrase
and submit_solution. The tasks not called by the flow yet cannot fail, so you can
run the file at any point. The skeleton is a suggestion: rename, split or add
tasks as your design needs, and keep your DAG sketch in step with them.
Use Prefect's logger (get_run_logger) for logging. Never hard-code your
computing ID or the endpoint: read them from the environment (.env).
"""
import os
import time

import boto3
from dbt.cli.main import dbtRunner 
import duckdb
import requests
from dotenv import load_dotenv
from prefect import flow, task, get_run_logger


load_dotenv()

UVA_ID = os.environ["UVA_ID"]
SCATTER_URL = f"http://127.0.0.1:{os.environ.get('SCATTER_PORT', '8000')}/api/scatter/{UVA_ID}"
DUCKDB_PATH = os.environ.get("DP2_DUCKDB", "dp2.duckdb")

sqs = boto3.client("sqs")  # the endpoint comes from AWS_ENDPOINT_URL_SQS


@task
def populate_queue() -> str:
    """POST to the scatter API and return your queue URL."""
    logger = get_run_logger()
    # TODO (Checkpoint A)
    
    uva_id = os.getenv("UVA_ID")
    port = os.getenv("SCATTER_PORT", "8000")
    endpoint = f"http://127.0.0.1:{port}/api/scatter/{uva_id}"

    response = requests.post(endpoint)
    response.raise_for_status()

    data = response.json()
    queue_url = data["sqs_url"]

    logger.info(f"Queue URL: {queue_url}")
    return queue_url

def get_counts(queue_url: str) -> dict:
    """Plain helper (not a task): return the three ApproximateNumberOf... counters as integers."""
    # TODO (Checkpoint A)
    sqs = boto3.client("sqs")
    response = sqs.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=[
            "ApproximateNumberOfMessages",
            "ApproximateNumberOfMessagesNotVisible",
            "ApproximateNumberOfMessagesDelayed",
        ],
    )
    attrs = response.get("Attributes", {})
    return {
        "visible": int(attrs.get("ApproximateNumberOfMessages", 0)),
        "not_visible": int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0)),
        "delayed": int(attrs.get("ApproximateNumberOfMessagesDelayed", 0)),
    }

@task
def prepare_landing() -> None:
    with duckdb.connect(DUCKDB_PATH) as con:
        con.execute("create schema if not exists raw")
        con.execute("create or replace table raw.fragments ("
                    "order_no varchar, word varchar, "
                    "received_at timestamp)")
        con.execute(
        "CREATE OR REPLACE TABLE raw.rejects ("
        "body VARCHAR, reason VARCHAR, "
        "received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

def stored_count(con) -> int:
    return con.execute("select count(distinct order_no) "
                       "from raw.fragments").fetchone()[0]


@task
def monitor_queue(queue_url: str, poll_s: int = 5, timeout_s: int = 1200) -> dict:
    """Log the three counters every poll_s seconds until nothing is delayed; raise after timeout_s."""
    # TODO (Checkpoint A)
    logger = get_run_logger()
    start_time = time.time()

    while True:
        if time.time() - start_time > timeout_s:
            raise TimeoutError(
                f"Queue monitoring exceeded timeout limit of {timeout_s} seconds."
            )

        counts = get_counts(queue_url)
        logger.info(
            f"Queue Counts -> Visible: {counts['visible']}, Not Visible:"
            f" {counts['not_visible']}, Delayed: {counts['delayed']}"
        )

        if counts["delayed"] == 0:
            logger.info("All delayed messages have arrived (Delayed: 0).")
            return counts
        time.sleep(poll_s)

@task
def collect_messages(queue_url: str, expected: int = 21, timeout_s: int = 1200) -> int:
    """Receive, land in DuckDB, then delete. Return how many fragments are stored."""
    logger = get_run_logger()
    deadline = time.monotonic() + timeout_s

    with duckdb.connect(DUCKDB_PATH) as con:
        while time.monotonic() < deadline:
            stored = stored_count(con)
            c = get_counts(queue_url)

            logger.info(
                "stored %d/%d | visible=%d not_visible=%d delayed=%d",
                stored,
                expected,
                c["visible"],
                c["not_visible"],
                c["delayed"],
            )

            # Stop when all 21 distinct fragments are stored and queue is empty
            if stored >= expected and (c["visible"] + c["not_visible"] + c["delayed"] == 0):
                logger.info(
                    "Collection complete: %d fragments stored, queue completely drained.",
                    stored,
                )
                return stored

            resp = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=10,
                MessageAttributeNames=["All"],
                WaitTimeSeconds=5,
            )
            messages = resp.get("Messages", [])

            for msg in messages:
                receipt_handle = msg["ReceiptHandle"]
                attrs = msg.get("MessageAttributes", {})

                # Correct single lookup for StringValue
                order_no = attrs.get("order_no", {}).get("StringValue")
                word = attrs.get("word", {}).get("StringValue")

                # Validate fragment: must have both attributes, and order_no must be numeric
                if order_no and word and str(order_no).strip().isdigit():
                    con.execute(
                        "INSERT INTO raw.fragments VALUES (?, ?, current_timestamp)",
                        [str(order_no).strip(), str(word).strip()],
                    )
                else:
                    # Poison message: archive in raw.rejects
                    logger.warning(
                        "Poison message detected (attrs: %s). Moving to raw.rejects.",
                        attrs,
                    )
                    con.execute(
                        "INSERT INTO raw.rejects (body, reason) VALUES (?, ?)",
                        [msg.get("Body", ""), f"Poison attributes: {attrs}"],
                    )

                # Always delete every message from SQS (both valid fragments and poison)
                sqs.delete_message(
                    QueueUrl=queue_url,
                    ReceiptHandle=receipt_handle,
                )

        stored = stored_count(con)

    raise TimeoutError(
        f"Only {stored}/{expected} fragments arrived before the timeout of {timeout_s} s"
    )

@task
def dbt_build() -> None:
    """Run `dbt build --project-dir dbt --profiles-dir dbt`; raise if anything fails."""
    # TODO (Checkpoint C)
    res = dbtRunner().invoke(["build", "--project-dir", "dbt",
                              "--profiles-dir", "dbt"])
    if not res.success:
        raise RuntimeError("dbt tests failed: not submitting")
    get_run_logger().info("dbt build passed: all tests green")

@task
def read_phrase() -> str:
    """Read the phrase from the dbt mart model in DuckDB."""
    # TODO (Checkpoint C)
    with duckdb.connect(DUCKDB_PATH) as con:
        n, phrase = con.execute("select fragment_count, phrase "
                                "from assembled_phrase").fetchone()
        get_run_logger().info("Assembled %d fragments: %s", n, phrase)
        return phrase 

@task
def submit_solution(phrase: str, platform: str = "prefect") -> int:
    """Send to dp2-submit with uvaid / phrase / platform attributes. Return the HTTP status code."""
    # TODO (Checkpoint D)
    url = sqs.get_queue_url(QueueName="dp2-submit")["QueueUrl"]
    resp = sqs.send_message(
        QueueUrl=url, MessageBody="dp2 solution",
        MessageAttributes={
            "uvaid": {"DataType": "String", "StringValue": UVA_ID},
            "phrase": {"DataType": "String", "StringValue": phrase},
            "platform": {"DataType": "String", "StringValue": platform}})
    status = resp["ResponseMetadata"]["HTTPStatusCode"]
    message_id = resp["MessageId"]
    get_run_logger().info("Submitted, HTTP %s | Receipt Code (MessageId): %s", status, message_id)
    if status != 200:
        raise RuntimeError(f"Submission returned HTTP {status}")
    return status

@flow(name="dp2-pipeline", log_prints=True)
def dp2_pipeline():
    queue_url = populate_queue()
    monitor_queue(queue_url)
    # TODO (Lesson 10): collect_messages -> dbt_build -> read_phrase -> submit_solution
    prepare_landing()
    collect_messages(queue_url)
    dbt_build()
    phrase = read_phrase()
    submit_solution(phrase)

if __name__ == "__main__":
    dp2_pipeline()
