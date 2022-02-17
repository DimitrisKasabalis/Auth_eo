### Setting up
#### Copy Auxilary Files from GoogleDrive to project folder
1. Download (as zip ) the folder https://drive.google.com/drive/folders/1OsCWe63xhzlwZaStDM21xW3ePr2W9Dwy
2. (unzip) the contents inside the local_files/aux_files folder

### Start the stack

#### Database and Broker 
```powershell
# start database worker broker
# -d flag daemonise the process
docker-compose up db rabbit -d  
```


```powershell
# Start the a worker.
# Workers connect at the broker and do all the work. We can have more than one worker
docker-compose up worker 
```

#### make the database tables

```powershell
docker-compose run --rm worker migrate
```

#### Load initial data

```powershell
docker compose run --rm worker loaddata 001-credentials.yaml 002-eo_groups.yaml 003-product_groups.yaml 004-source_groups.yaml 005-pipelines.yaml
```



### Ops

#### Start a scrape

```powershell
docker-compose run --rm worker scrape  <name-of-spider> # or
docker-compose run --rm worker scrape  --as-task <name-of-spider>  # to sent it to worker as task


#### Download a data source

```powershell
docker-compose run --rm worker download  <name-or-id-of-resource> # or
docker-compose run --rm worker download  --as-task <name-or-id-of-resource>
```

#### Make a product

```powershell
# TODO: 
docker compose rm --rm worker make_product <name-or-id-of-product> # 
docker compose rm --rm worker make_product --as-task <name-or-id-of-product>
```

#### Service

```powershell
# bring up broker-scheduler-worker-db stack
docker compose up
```

#### To do:

- make base image, 5gigs upload is too much
- implement ftp download
- tests-tests-tests
- add product
- delete sources
- ease of use

##### Low prio

- email when a task failing
- check if file was downloaded correctly
- auxiliary data
