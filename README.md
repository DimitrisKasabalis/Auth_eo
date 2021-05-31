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
docker-compose run --rm worer scrape  <name-of-spider> # or
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
docker-compose run --rm download  <name-or-id-of-resource> # or
docker-compose run --rm download  --as-task <name-or-id-of-resource>

# eg:

```

### Bake a product
