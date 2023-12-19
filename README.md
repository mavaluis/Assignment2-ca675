# Assignment2-ca675
GitHub repository for Assignment 2 (CA 675)

**Creating a bucket**
- Search buckets in GCP search bar
- Press create (+) button on the top of the screen
  **Name Bucket**:
  - ca675-assigntment2-bucket
  **Choose where to store data**:
  - Location: us (Multiple regions in United States)
  - Location type: Multi-region
  **Choose storage class for your data**:
  - Default storage class: Standard
  **Choose how to control access to object**:
  - Public access prevention: On
  - Access control: Uniform
  **Choose how to protect object data**:
  - Protection tools: None
  - Data encryption: Google-managed

Click create button on the bottom
Put the folders in this github file into the bucekt

**Creating dataproc nodes**
- Search dataproc in GCP search bar
- Click create cluster (+) button on the top of the screen
    - Choose Cluster on Compute Engine
    **Set up Cluster**:
    - Name: Set to anything (cluster-assignment2-spark was name I worked with)
    - Cluster type: Standard (1 Master, N workers)
    - Versioning: Anything
      **Configure nodes**:
        **Manager node**:
        - General Purpose
        - Series: N1
        - Machine type: n1-standard-2
        - CPU Platform/GPU: don't change anyhting
        **Worker nodes**:
        - General Purpose
        - Series: N1
        - Machine type: n1-standard-2
        - Number of worker nodes: 2
      **Customize Cluster**:
      - Scheduled deletion:
          - Delete after a cluster idle time period without submitted jobs: 2 hours
      - Cloud storage staging bucket:
          - bucket you created
      **Manage security**:
      - Project access:
          - Check off: Enables the cloud-platform scope for this cluster

Once done click the create button


**How to use the scripts**
You have to make sure that the /output directory in the bucket is empty, or else the scripts won't work.

To use the scripts you need to write the following:
    gcloud dataproc jobs submit pyspark     --project <project name>    --cluster=<cluster name>   --region=<region>   gs://ca675-assignment2-bucket/pySpark-scripts/<script in specific you want to run>

Let's say I want to run dataAggregationHSL.py this is what I would type in:
    gcloud dataproc jobs submit pyspark     --project citric-alliance-408023     --cluster=cluster-assignment2-spark     --region=us-east1     gs://ca675-assignment2-bucket/pySpark-scripts/dataAggregationHSL.py
    
