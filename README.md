### Start the stack

```powershell
# start database worker broker
docker-compose up
```

#### make the database tables

```powershell
docker-compose run --rm worker migrate
```

### Ops

#### Start a scrape

```powershell
docker-compose run --rm worker scrape  <name-of-spider> # or
docker-compose run --rm worker scrape  --as-task <name-of-spider>  # to sent it to worker as task

# spiders
###  Lai
# lai-1km-v2-spider
# lai-1km-global-v1-spider
# lai-300m-v1-spider

###  NDVI
# ndvi-300m-v1-spider
# ndvi-300m-v2-spider
# ndvi-1km-v1-spider
# ndvi-1km-v2-spider
# ndvi-1km-v3-spider

#### VCI
# vci-v1-spider
# wb-africa-v1-spider
```

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
