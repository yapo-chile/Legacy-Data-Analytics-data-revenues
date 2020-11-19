# incremental-product-order pipeline 

# incremental-product-order

## Description

Process "product order" its a step of incremental data process that get data of puchases (and details) from blocket db and leave datawarehouse stg schema in a first step. Then, get data ingested in stg joining with ods.product and ods.ad and generate table ods.product_order final table

## Pipeline Implementation Details

|   Field           | Description                                                                |
|-------------------|----------------------------------------------------------------------------|
| Input Source      | Blocket: purchase_detail, purchase, payment_groups from schema public, blocket_2020 & blocket_2019 DW: stg.product_order, ods.product and ods.ad|
| Output Source     | Datawarehouse: stg.product_order and ods.product_order                  |
| Schedule          | --:--                                                                      |
| Rundeck Access    | Data_jobs          |
| Associated Report | N/A                        |


### Build
```
make docker-build
```

### Run micro services
```
docker run -v /local-path/secrets/pulse:/app/pulse-secret \
           -v /local-path/secrets/db-secret:/app/db-secret \
           -e APP_PULSE_SECRET=/app/pulse-secret \
           -e APP_DB_SECRET=/app/db-secret \
           containers.mpi-internal.com/yapo/incremental-product-order:[TAG]
```

### Run micro services with parameters

```
docker run -v /local-path/secrets/pulse:/app/pulse-secret \
           -v /local-path/secrets/db-secret:/app/db-secret \
           -e APP_PULSE_SECRET=/app/pulse-secret \
           -e APP_DB_SECRET=/app/db-secret \
           containers.mpi-internal.com/yapo/incremental-product-order:[TAG] \
           -date_from=YYYY-MM-DD \
           -date_to=YYYY-MM-DD
```

### Adding Rundeck token to Travis

If we need to import a job into Rundeck, we can use the Rundeck API
sending an HTTTP POST request with the access token of an account.
To add said token to Travis (allowing travis to send the request),
first, we enter the user profile:
```
<rundeck domain>:4440/user/profile
```
And copy or create a new user token.

Then enter the project settings page in Travis
```
htttp://<travis server>/<registry>/<project>/settings
```
And add the environment variable RUNDECK_TOKEN, with value equal
to the copied token
