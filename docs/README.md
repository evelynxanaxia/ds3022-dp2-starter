Put your DAG sketch (`dag.jpg`), dashboard screenshots and dbt test screenshots here.

# Documentation & Deliverables

## DAG Sketch
![DAG Sketch](dag.jpg)

## Prefect Dashboard Run
![Prefect Dashboard Run](Flow_run.png)

## Polling Strategy
The pipeline implements an active polling mechanism using boto3 (`get_queue_attributes`) to query three key SQS queue metrics:
- `ApproximateNumberOfMessages`
- `ApproximateNumberOfMessagesNotVisible`
- `ApproximateNumberOfMessagesDelayed`

The `monitor_queue` task inspects these counters at regular intervals (`poll_s=5`). It blocks downstream processing until `ApproximateNumberOfMessagesDelayed == 0`, ensuring all fragments have been released into the queue by the Scatter API before ingestion begins. A timeout ceiling (`timeout_s=1200`) guards against infinite execution loops.

The `populate_queue`  and `monitor_queue`take the inputs of the UVA_ID and the scatter URL from the .env folder, which then ouputs the queue URL. One thing to keep in mind is that you should not re-run these tasks after the collecting has begun because doing this would empty the queue.

The  `collect_messages` task takes the queue URL as an input and successfully outputs 21 read rows in the raw.fragments. Afterwards the system ensures that no more items that need to be read remain. Therefore the queue is empty. This task can be rerun because it saves things before deleting anything and retrying it would only show what is left in the queue, which should be nothing if the task read everything in the queue.

 `dbt_build` uses the raw.fragments (unorganized words) as an input and then outputs them assembled. The task ensures that the test results are accurate and that no fragments that are needed are missing. This task  cannot be rerun in case of an error because it wouldn't change the answer. If there is an error at this task review the ones before.

 The `read_phrase` task takes the assembled phrases from  `dbt_build`and returns the phrases as strings, which classifies them as text. This task can be rerun but there's really no need because it can only run if  `dbt_build` ran before this task, so if there's any errors it would most likely come from  `dbt_build`.

Lastly the  `submit_solution`task takes the phrases and UVA_ID as inputs and outputs the HTTP status link/ If there is an error like a netwrok glitch it can be rerun so that it returns a working link. 